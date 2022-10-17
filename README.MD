# Async csv uploader to PG

## Check list

 - [x] Upload to PG
 - [ ] Migration script
 - [ ] Find by DATE
 - [ ] Sort missed|changed records

## Required

 - install libs
```bash
pip install -r ./requirements.txt
```
 - Preinstalled Postgres with permissions and tables ( check `postgres/init.sql`)
## How to run
- Add db congig creds on `config.json` file
- Run `python3 main.py`