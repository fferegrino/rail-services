Download the file

```bash
curl -L \
  -u "$NR_USERNAME:$NR_PASSWORD" \
  -o data/schedule.json.gz \
  "https://publicdatafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full"
```


Load the file into the database

```bash
python scripts/load.py
```
