This tool scans the JDK on which it is running to extract its public accessible API.
That is:
- public methods (including constructors) of public, exported classes as well as protected methods of these if not final.
- internal implementations (overwrites) of above.

The output of this tool is meant to be diffed against the output for another JDK
version to identify changes that need to be reviewed for entitlements.

Usage example:
```bash
./gradlew :libs:entitlement:tools:jdk-api-extractor:run -Druntime.java=24 --args="api-jdk24.tsv"
./gradlew :libs:entitlement:tools:jdk-api-extractor:run -Druntime.java=25 --args="api-jdk25.tsv"
diff libs/entitlement/tools/jdk-api-extractor/api-jdk24.tsv libs/entitlement/tools/jdk-api-extractor/api-jdk25.tsv
```

To review the diff of deprecations (by means of `@Deprecated`), use `--deprecations-only` as 2nd argument.

```bash
./gradlew :libs:entitlement:tools:jdk-api-extractor:run -Druntime.java=24 --args="deprecations-jdk24.tsv --deprecations-only"
```
