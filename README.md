A script to download all attachment files from a Discord data export package. It extracts all URLs seemingly pointing at the CDN, downloads the files, and organizes them by file type.
Written for Linux but should work under WSL for Windows?

-   ✅ Extracts attachment URLs by searching through Discord export files (may include URLs to other files, from other users?)

-   ✅ Renames files to avoid duplicates (there's a lot of 'unknown.png')

-   ✅ Downloads attachments found concurrently

-   ✅ Organizes downloaded files by file type into folders

```bash
python dripcord.py --package /path/to/discord/export --output /downloads
```
