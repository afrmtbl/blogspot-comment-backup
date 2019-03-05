import asyncio, aiohttp, json, tldextract, sys, os, logging, signal, time, gzip, shutil

from aiohttp import FormData

sys.path.insert(0, './fetch/')

from fetch.posts import get_blog_posts, MarkExclusion, NoEntries
from downloader import PostsDownloader
from batch_file import BatchFile

BATCH_DOWNLOADER_COUNT = 1

MASTER_SERVER = "http://blogspot-comments-master.us.to"
UPLOAD_SERVER = "http://blogstore.bot.nu"

WORKER_VERSION = 3
# WORKER_BATCH_SIZE = 500

GET_ID_ENDPOINT = f"{MASTER_SERVER}/worker/getID&=worker_version={WORKER_VERSION}"
# worker id must be provided as a query parameter: id={ID}
GET_BATCH_ENDPOINT = f"{MASTER_SERVER}/worker/getBatch"
SUBMIT_EXCLUSION_BLOG_ENDPOINT = f"{MASTER_SERVER}/worker/submitExclusion"
SUBMIT_DELETED_BLOG_ENDPOINT = f"{MASTER_SERVER}/worker/submitDeleted"
SUBMIT_PRIVATE_BLOG_ENDPOINT = f"{MASTER_SERVER}/worker/submitPrivate"
SUBMIT_CUSTOM_DOMAIN_ENDPOINT = f"{MASTER_SERVER}/worker/submitDomain"

UPDATE_BATCH_ENDPOINT = f"{MASTER_SERVER}/worker/updateStatus"
DOMAINS_LIST_ENDPOINT = f"{UPLOAD_SERVER}/worker/domains.txt.gz"

SUBMIT_BATCH_UNIT = f"{UPLOAD_SERVER}/submitBatchUnit"
# called by master
# VERIFY_BATCH_UNIT = f"{UPLOAD_SERVER}/getVerifyBatchUnit"

# Stop trying to connect to master after 18 hours
MASTER_SLEEP_TOTAL = (60 * 60) * 18
# MASTER_SLEEP_TOTAL = 10
# Multiply the sleep amount each retry
# MASTER_SLEEP_INCREMENT = 5
MASTER_SLEEP_INCREMENT = 30
# Stop multiplying after 180 seconds
# MASTER_SLEEP_MAXIMUM = 10
MASTER_SLEEP_MAXIMUM = 180

class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, signum, frame):
    self.kill_now = True

async def get_worker_id(session):

    def fail_func(response_status):
        print(f"[get_worker_id] The server response was unsuccessful ({response_status}), unable to get a worker ID")

    response = await retry_request_on_fail(session.get, fail_func, True, False, GET_ID_ENDPOINT)
    if response and response.status == 200:
        text = await response.text()
        return text

async def get_batch(worker_id, session):

    def fail_func(response_status):
        print(f"[get_batch] The server response was unsuccessful ({response_status}), unable to get a batch")

    params = {"id": worker_id}
    response = await retry_request_on_fail(session.get, fail_func, True, True, GET_BATCH_ENDPOINT, params=params)
    if response and response.status == 200:
        text = await response.text()
        obj = json.loads(text)
        return {
            "batch_id": obj["batchID"], 
            "random_key": obj["randomKey"], 
            "file_offset": obj["offset"], 
            "exclusion_limit": obj["limit"],
            "batch_type": obj["assignmentType"],
            "content": obj["content"],
            "batch_size": obj["batchSize"]
        }

async def update_batch_status(worker_id, batch_id, random_key, status, session):
    def fail_func(response_status):
        print(f"[update_batch_status] The server response was unsuccessful ({response_status}), unable to update batch status")

    params = {
        "id": worker_id, 
        "batchID": batch_id, 
        "randomKey": random_key,
        "status": status
    }
    response = await retry_request_on_fail(session.get, fail_func, True, False, UPDATE_BATCH_ENDPOINT, params=params)
    if response and response.status == 200:
        print(f"Successfully updated batch status: worker_id: {worker_id} | batch_id: {batch_id}")
        return True


async def submit_batch_exception(exception_type, endpoint, worker_id, batch_id, random_key, blog_name, session, domain=None):

    variables_string = f"worker_id: {worker_id} batch_id: {batch_id} random_key: {random_key} blog_name: {blog_name}"

    def fail_func(response_status):
        print(f"[submit_batch_exception] The server response was unsuccessful ({response_status}), unable to submit as {exception_type}\n{variables_string}")

    params = {
        "id": worker_id, 
        "batchID": batch_id, 
        "randomKey": random_key
    }

    if exception_type == "domain":
        params["domain"] = domain
        params["blog"] = blog_name
    else:
        params[exception_type] = blog_name

    response = await retry_request_on_fail(session.get, fail_func, True, False, endpoint, params=params)

    if response and response.status == 200:
        text = await response.text()
        success = text == "Success"
        if success:
            print(f"Submitted batch for {exception_type}\n{variables_string}")
            return True
        elif text == "Dupe":
            print(f"Aborting submitting as {exception_type}, duplication detected\n{variables_string}")
            return True
        else:
            print(f"Failed to submit {exception_type}\n{variables_string}")
            return False

async def submit_exclusion(worker_id, batch_id, random_key, blog_name, session):
    success = await submit_batch_exception("exclusion", SUBMIT_EXCLUSION_BLOG_ENDPOINT, worker_id, batch_id, random_key, blog_name, session)
    return success

async def submit_private(worker_id, batch_id, random_key, blog_name, session):
    success = await submit_batch_exception("private", SUBMIT_PRIVATE_BLOG_ENDPOINT, worker_id, batch_id, random_key, blog_name, session)
    return success

async def submit_deleted(worker_id, batch_id, random_key, blog_name, session):
    success = await submit_batch_exception("deleted", SUBMIT_DELETED_BLOG_ENDPOINT, worker_id, batch_id, random_key, blog_name, session)
    return success

async def submit_custom_domain(worker_id, batch_id, random_key, blog_name, domain, session):
    success = await submit_batch_exception("domain", SUBMIT_CUSTOM_DOMAIN_ENDPOINT, worker_id, batch_id, random_key, blog_name, session, domain=domain)
    return success

async def upload_batch(worker_id, batch_id, random_key, version, file_path, file_name, session):

    def fail_func(response_status):
        print(f"[upload_batch] The server response was unsuccessful ({response_status}), unable to upload batch")

    async def create_request(url):
        data = FormData()
        data.add_field("workerID", str(worker_id))
        data.add_field("batchID", str(batch_id))
        data.add_field("batchKey", str(random_key))
        data.add_field("version", str(version))
        data.add_field("data", open(file_path, "rb"), filename=file_name, content_type="application/x-gzip")
        response = await session.post(url, data=data)
        return response

    url = SUBMIT_BATCH_UNIT

    # response = await retry_request_on_fail(create_request, fail_func, False, False, url)
    response = await create_request(url)
    if response.status == 200:
        print(f"Successfully uploaded batch: worker_id: {worker_id} batch_id: {batch_id} | file_path: {file_path}")
        return True
    else:
        print(f"Unable to upload batch: worker_id: {worker_id} batch_id: {batch_id} | file_path: {file_path}")
        return False

async def download_batch(worker_id, batch_id, batch_type, batch_content, random_key, batch_size, offset, domains, exclusion_limit, session):

    domains.seek(offset)

    file_path = "../output/"
    batch_file = BatchFile(file_path, batch_id)

    async def download_blog(blog_name, first_blog):

        if killer.kill_now:
            print(f"Graceful Killer enabled, setting batch status to Fail | batch_id: {batch_id}")
            await update_batch_status(worker_id, batch_id, random_key, "f", session)
            exit(1)
        else:
            try:
                print(f"Downloading blog: {blog_name}")
                blog_posts = await get_blog_posts(f"https://{blog_name}.blogspot.com", exclusion_limit, session)
                for i, post in enumerate(blog_posts):
                    if post.startswith("https:///"):
                        blog_posts[i] = post.replace("https://", f"https://{blog_name}.blogspot.com")
                # The blog cannot be found / is deleted
                if blog_posts == "nf":
                    print(f"Marking as deleted: batch_id: {batch_id} | blog_name: {blog_name}")
                    await submit_deleted(worker_id, batch_id, random_key, blog_name, session)
                    blog_domain = f"{blog_name}.blogspot.com"
                    batch_file.start_blog(WORKER_VERSION, blog_name, blog_domain, "d", first_blog)
                    batch_file.end_blog()
                # The blog is private
                elif blog_posts == "pr":
                    print(f"Marking as private: batch_id: {batch_id} | blog_name: {blog_name}")
                    await submit_private(worker_id, batch_id, random_key, blog_name, session)
                    blog_domain = f"{blog_name}.blogspot.com"
                    batch_file.start_blog(WORKER_VERSION, blog_name, blog_domain, "p", first_blog)
                    batch_file.end_blog()
                # Other errors
                elif blog_posts == "oe":
                    if batch_type == "list":
                        print(f"Marking as exclusion: batch_id: {batch_id} | blog_name: {blog_name}")
                        await submit_exclusion(worker_id, batch_id, random_key, blog_name, session)
                        blog_domain = f"{blog_name}.blogspot.com"
                        batch_file.start_blog(WORKER_VERSION, blog_name, blog_domain, "e", first_blog)
                        batch_file.end_blog()
                    elif batch_type == "domain":
                        print(f"Marking as investigate: batch_id: {batch_id} | blog_name: {blog_name}")
                        blog_domain = f"{blog_name}.blogspot.com"
                        batch_file.start_blog(WORKER_VERSION, blog_name, blog_domain, "__i", first_blog)
                        batch_file.end_blog()
                else:
                    blog_tld = tldextract.extract(blog_posts[0])
                    blog_domain = f"{blog_tld.domain}.{blog_tld.suffix}"
                    if blog_tld.subdomain:
                        blog_domain = f"{blog_tld.subdomain}.{blog_domain}"

                    if blog_domain != f"{blog_name}.blogspot.com":
                        print(f"Marking as custom domain: batch_id: {batch_id} | blog_name: {blog_name} | blog_domain: {blog_domain}")
                        await submit_custom_domain(worker_id, batch_id, random_key, blog_name, blog_domain, session)

                    batch_file.start_blog(WORKER_VERSION, blog_name, blog_domain, "a", first_blog)
                    dler = PostsDownloader(blog_posts, batch_file, exclusion_limit)
                    await dler.start()
                    batch_file.end_blog()

            except MarkExclusion:
                if batch_type == "list":
                    print(f"Marking as exclusion: batch_id: {batch_id} | blog_name: {blog_name}")
                    await submit_exclusion(worker_id, batch_id, random_key, blog_name, session)
                    blog_domain = f"{blog_name}.blogspot.com"
                    batch_file.start_blog(WORKER_VERSION, blog_name, blog_domain, "e", first_blog)
                    batch_file.end_blog()
                elif batch_type == "domain":
                    print(f"Marking as investigate: batch_id: {batch_id} | blog_name: {blog_name}")
                    blog_domain = f"{blog_name}.blogspot.com"
                    batch_file.start_blog(WORKER_VERSION, blog_name, blog_domain, "__i", first_blog)
                    batch_file.end_blog()
            except NoEntries:
                print(f"Blog has no posts: batch_id: {batch_id} | blog_name: {blog_name}")
                blog_domain = f"{blog_name}.blogspot.com"
                batch_file.start_blog(WORKER_VERSION, blog_name, blog_domain, "a", first_blog)
                batch_file.end_blog()

    if batch_type == "list":
        # batch_size = 5
        print("Downloading multiple domains (list)")
        for i in range(batch_size):
            print(f"[BATCH PROGRESS] {i}/{batch_size}")
            blog_name = domains.readline().replace("\n", "")
            if blog_name != "":
                first_blog = (i == 0)
                await download_blog(blog_name, first_blog)
            else:
                print("Reached end of domains list")

    elif batch_type == "domain":
        if batch_content != "":
            print(f"Downloading single domain: {batch_content}")
            await download_blog(batch_content, True)
        else:
            raise Exception(f"Invalid batch_content: {batch_content}")
    else:
        raise Exception("Invalid batch_type")

    batch_file.end_batch()

    file_path = batch_file.directory + batch_file.file_name
    file_name = batch_file.file_name

    upload_response = await upload_batch(worker_id, batch_id, random_key, WORKER_VERSION, file_path, file_name, session)
    await update_batch_status(worker_id, batch_id, random_key, "c" if upload_response else "f", session)
    print(f"Deleting batch file | file_path: {file_path} | status: {upload_response}")
    os.remove(file_path)

    return True

async def retry_request_on_fail(func, fail_func, check_text, check_batch_fail=False, *args, **kwargs):

    total_slept = 0
    sleep_amount = MASTER_SLEEP_INCREMENT

    while total_slept < MASTER_SLEEP_TOTAL:
        try:
            response = await func(*args, **kwargs)
            if check_batch_fail:
                text = await response.text()
                if text and text != "Fail" and response.status == 200:
                    obj = json.loads(text)
                    if "batchID" in obj and obj["batchID"] != "Fail":
                        return response
                    else:
                        fail_func(response.status)
                        print(f"Retrying request | sleep_amount: {sleep_amount} total_slept: {total_slept}")
                        await asyncio.sleep(sleep_amount)
                        total_slept += sleep_amount
                        if sleep_amount < MASTER_SLEEP_MAXIMUM:
                            sleep_amount += MASTER_SLEEP_INCREMENT

                else:
                    fail_func(response.status)
                    print(f"Retrying request | sleep_amount: {sleep_amount} total_slept: {total_slept}")
                    await asyncio.sleep(sleep_amount)
                    total_slept += sleep_amount
                    if sleep_amount < MASTER_SLEEP_MAXIMUM:
                        sleep_amount += MASTER_SLEEP_INCREMENT
            elif check_text:
                text = await(response.text())
                print(f"Server response: {text}")
                if (text != "Fail" or text == "Dupe") and response.status == 200:
                    # print("Success!")
                    return response
                else:
                    fail_func(response.status)
                    print(f"Retrying request | sleep_amount: {sleep_amount} total_slept: {total_slept}")
                    await asyncio.sleep(sleep_amount)
                    total_slept += sleep_amount
                    if sleep_amount < MASTER_SLEEP_MAXIMUM:
                        sleep_amount += MASTER_SLEEP_INCREMENT

            elif response.status == 200:
                print("Server responded with 200")
                return response
            else:
                fail_func(response.status)
                print(f"Retrying request | sleep_amount: {sleep_amount} total_slept: {total_slept}")
                await asyncio.sleep(sleep_amount)
                total_slept += sleep_amount
                if sleep_amount < MASTER_SLEEP_MAXIMUM:
                    sleep_amount += MASTER_SLEEP_INCREMENT
        except Exception:
            fail_func("unknown")
            print(f"Retrying request | sleep_amount: {sleep_amount} total_slept: {total_slept}")
            await asyncio.sleep(sleep_amount)
            total_slept += sleep_amount
            if sleep_amount < MASTER_SLEEP_MAXIMUM:
                sleep_amount += MASTER_SLEEP_INCREMENT


    print(f"Request retry reached max ({MASTER_SLEEP_TOTAL} seconds)")
    sys.exit(1)
    # return False

async def download_domains():

    class IncompleteDomains(Exception):
        pass

    async with aiohttp.ClientSession() as session:

        def fail_func(response_status):
            print(f"Failed to get domains.txt.gz, retrying")

        try:

            domains_response = await retry_request_on_fail(session.get, fail_func, False, False, DOMAINS_LIST_ENDPOINT)

            domains_length = int(domains_response.headers["Content-Length"])

            print(f"Received content length from server: {domains_length}")

            with open("../domains.txt.gz", "wb") as domains:
                total_bytes_downloaded = 0
                while True:
                    # download in chunks of 10MB
                    chunk = await domains_response.content.read((1000 * 1000) * 10)
                    chunk_length = len(chunk)
                    total_bytes_downloaded += chunk_length
                    if total_bytes_downloaded == domains_length and chunk_length == 0:
                        break
                    elif chunk_length == 0:
                        print("Sleeping", total_bytes_downloaded, domains_length, total_bytes_downloaded == domains_length)
                        time.sleep(2)
                    else:
                        domains.write(chunk)

            final_file_size = os.path.getsize("../domains.txt.gz")
            print(f"domains.txt.gz | Download finished (expected size: {domains_length}, got: {final_file_size})")
            if final_file_size == domains_length:
                print(f"Successfully downloaded domains.txt (expected size: {domains_length}, got: {final_file_size})")
                print("Extracting to ../domains.txt")
                with gzip.open("../domains.txt.gz", "rb") as f_in:
                    with open("../domains.txt", "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                if os.path.exists("../domains.txt.gz"):
                    print("Deleting gzip..")
                    os.remove("../domains.txt.gz")
            else:
                print(f"Domains list is incomplete. Try manually downloading in a browser\n{DOMAINS_LIST_ENDPOINT}")
                if os.path.exists("../domains.txt.gz"):
                    print("Deleting gzip..")
                    os.remove("../domains.txt.gz")
                raise IncompleteDomains("The domains list is incomplete")
        except asyncio.TimeoutError:
            print("Request timed out..")
            print(f"Delete domains.txt and start the worker again, or try manually downloading and extracting the domains list from {DOMAINS_LIST_ENDPOINT}")
            print(f"Should that also fail to download, try the same with https://archive.org/details/domains.txt")
            exit(0)


async def batch_downloader(worker_id, domains, session, batch_id):
    while True:
        print("Requesting new batch...")
        batch = await get_batch(worker_id, session)
        # batch = {"batch_id": 11580, "batch_type": "domain", "random_key": 2938, "content": "kalaichotkovai", "batch_size": 250, "file_offset": 0, "exclusion_limit": 0}
        print(f"Received batch: {batch}")
        if batch:
            batch_id = batch["batch_id"]
            batch_type = batch["batch_type"]
            random_key = batch["random_key"]
            batch_content = batch["content"]
            batch_size = batch["batch_size"]
            offset = int(batch["file_offset"])
            exclusion_limit = int(batch["exclusion_limit"])

            batch_result = None

            for i in range(3):
                try:
                    batch_result = await download_batch(worker_id, batch_id, batch_type, batch_content, random_key, batch_size, offset, domains, exclusion_limit, session)
                    break
                except Exception as e:
                    print(f"Error: {e}\nRetrying downloading of batch in 10 seconds: batch_id: {batch_id}")
                    await asyncio.sleep(10)

            if not batch_result:
                print(f"Unable to download batch | batch_id: {batch_id}, requesting new batch in 10 seconds")

        else:
            print("Unable to get batch, requesting new batch in 10 seconds")

        await asyncio.sleep(10)


async def main():

    # logging.basicConfig(format="%(message)s", level=logging.INFO)

    with open("../domains.txt", "r") as domains:
        async with aiohttp.ClientSession() as session:
            print("Requesting worker ID")
            worker_id = await get_worker_id(session)
            # worker_id = "27747438-9825-51e1-9578-8807297944e6"
            if worker_id:
                batch_downloader_tasks = []
                print(f"Received worker ID: {worker_id}")
                for i in range(BATCH_DOWNLOADER_COUNT):
                    task = asyncio.create_task(batch_downloader(worker_id, domains, session, i))
                    batch_downloader_tasks.append(task)

                await asyncio.gather(*batch_downloader_tasks)
                print("All batch downloaders done")

if __name__ == '__main__':
    killer = GracefulKiller()

    # create the output folder for the gzipped batches
    if not os.path.isdir("../output"):
        os.makedirs("../output")

    # download the domains list
    if not os.path.exists("../domains.txt"):
        print("Downloading domains list..")
        asyncio.run(download_domains())
        if os.path.exists("../domains.txt.gz"):
            print("Deleting gzip..")
            os.remove("../domains.txt.gz")
    else:
        file_size = os.path.getsize("../domains.txt")
        expected_size = 122697503
        if file_size == expected_size:
            print("Found valid domains.txt")
        else:
            print(f"Domains list should be {expected_size} bytes, but it's {file_size} bytes")
            print("Trying to re download domains.txt..")
            try:
                asyncio.run(download_domains())
                if os.path.exists("../domains.txt.gz"):
                    print("Deleting gzip..")
                    os.remove("../domains.txt.gz")
            except:
                print("Failed to redownload domains list..")
                print(f"Delete domains.txt and start the worker again, or try manually downloading and extracting the domains list from {DOMAINS_LIST_ENDPOINT}")
                print(f"Should that also fail to download, try with https://archive.org/details/domains.txt")
                exit(0)


    asyncio.run(main())
