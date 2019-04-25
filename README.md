![logo](logo.png)
# blogspot-comment-backup
A project to archive comments from [blogspot](https://www.blogger.com/) blogs

\***Google+ Blogspot comments are no longer available. You can now download the data we were able to save. Thanks for the help!**

[Searchable Index](https://ia801405.us.archive.org/9/items/Blogspot_GPlus_Comments_Index/index.html) |
[archive.org](https://archive.org/details/Blogspot_GPlus_Comments)

### Running this worker:
\***python version 3.7 or later is required**

You can run this worker by running `python3 worker.py` from the `src` directory of this project. Python 3.7.2 is recommended and the `aiohttp` and `tldextract` modules are required. (You can install `aiohttp` and `tldextract` by running `pip install aiohttp tldextract`.)

This worker also runs on Heroku. To deploy to Heroku, follow these steps:
- Fork this repo
- Install the [pull](https://github.com/apps/pull) app to your fork for automatic worker updates as needed
- [Create a new app](https://dashboard.heroku.com/new-app) on Heroku
- Go to the *Deploy* tab on your Heroku app and link your GitHub repo fork. Enable automatic deploys.
- Go to the *Resources* tab on your Heroku app and ensure the *worker* dyno is enabled. (You may need to refresh the page to see the *worker* dyno option.

### Resource Cost
A worst case example of the cost of getting a single comment (single page)

(top comment on [this post](https://apis.google.com/u/0/_/widget/render/comments?first_party_property=BLOGGER&query=https://blogger.googleblog.com/2019/01/an-update-on-google-and-blogger.html))

Replies | Comment +1ers | Reply +1ers | JSON Size (kB) | Estimated Network Requests | Elapsed Time (seconds)
------- | ----------- | --------- | -------------- | -------------------------- | ------------
❌ | ❌ | ❌ | 5   | 1 | ~1.73
❌ | ✔️ | ❌ | 31  | 2 | ~2.13
✔️ | ❌ | ❌ | 111 | 2 | ~2.37
✔️ | ✔️ | ❌ | 134 | 3 | ~2.80
✔️ | ❌ | ✔️ | 137 | 65 | ~3.10
✔️ | ✔️ | ✔️ | 160 | 66 | ~3.10
