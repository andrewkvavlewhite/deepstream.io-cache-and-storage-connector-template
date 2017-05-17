# deepstream.io-storage-neo4j
A template that can be forked to create new cache and storage connectors

```yaml
plugins:
  storage:
    name: neo4j
    options:
      connectionString: ${NEO4J_CONNECTION_STRING}
      userName: ${NEO4J_USER_NAME}
      password: ${NEO4J_PASSWORD}
      splitChar: '/'
```
