# schema-linking

Create 2 schema links from one source cluster to 2 destination clusters

Requires a `config1.txt` and a `config2.txt` file with the following content in each.

```
schema.registry.url=<destination sr url>
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=<destination api key>:<destination api secret>
```