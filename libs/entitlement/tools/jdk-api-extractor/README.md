This tool scans the JDK on which it is running to extract its public accessible API.
That is:
- public methods (including constructors) of public, exported classes as well as protected methods of non-final classes.
- any overwrites of public methods of public, exported super classes and interfaces.

The output of this tool is meant to be diffed against the output for another JDK
version to identify changes that need to be reviewed for entitlements.
The output is compatible with the `public-callers-finder` tool to calculate the
public transitive surface of new additions. See the example below.

The following `TAB`-separated columns are written:
1. module name
2. unused / empty (for compatibility with `public-callers-finder`)
3. unused / empty (for compatibility with `public-callers-finder`)
4. fully qualified class name (ASM style, with `/` separators)
5. method name
6. method descriptor (ASM signature)
7. visibility (`PUBLIC` / `PROTECTED`)
8. `STATIC` modifier or empty
9. `FINAL` modifier or empty

Usage example:
```bash
./gradlew :libs:entitlement:tools:jdk-api-extractor:run -Druntime.java=24 --args="api-jdk24.tsv"
./gradlew :libs:entitlement:tools:jdk-api-extractor:run -Druntime.java=25 --args="api-jdk25.tsv"

# diff the public apis
diff -u libs/entitlement/tools/jdk-api-extractor/api-jdk24.tsv libs/entitlement/tools/jdk-api-extractor/api-jdk25.tsv > libs/entitlement/tools/jdk-api-extractor/api.diff

# extract additions in the new JDK, these require the most careful review
cat libs/entitlement/tools/jdk-api-extractor/api.diff | grep '^+[^+]' | sed 's/^+//' > api-jdk25-additions.tsv

# review new additions next for critical ones that should require entitlements
# once done, remove all lines that are not considered critical and run the public-callers-finder to report
# the transitive public surface for these additions to identify methods that are node already covered by entitlements
./gradlew :libs:entitlement:tools:public-callers-finder:run -Druntime.java=25 --args="api-jdk25-additions.tsv --transitive --check-instrumentation"
```

### Optional arguments:

- `--deprecations-only`: reports public deprecations (by means of `@Deprecated`)
- `--include-incubator`: include incubator modules (e.g. `jdk.incubator.vector`)

If `-Druntime.java` is not provided, the bundled JDK is used.

```bash
./gradlew :libs:entitlement:tools:jdk-api-extractor:run -Druntime.java=24 --args="deprecations-jdk24.tsv --deprecations-only"
```

### Comparing two patch releases of the same major version

`-Druntime.java` takes a **major** version and resolves it through the Gradle toolchain service, so
it cannot select between two patch releases of the same major — `-Druntime.java=26` may resolve to
any JDK 26 build present in the toolchain cache. Comparing, say, 26.0.1 against 26.0.2 with
`-Druntime.java=26` will silently scan the same JDK twice and produce an empty diff that looks like
a clean result.

To pin an exact build, extract each JDK and point `RUNTIME_JAVA_HOME` at it:

```bash
RUNTIME_JAVA_HOME=/path/to/jdk-26.0.1 ./gradlew :libs:entitlement:tools:jdk-api-extractor:run --args="api-jdk26.0.1.tsv"
RUNTIME_JAVA_HOME=/path/to/jdk-26.0.2 ./gradlew :libs:entitlement:tools:jdk-api-extractor:run --args="api-jdk26.0.2.tsv"
```

Each run prints `Writing result for <version>`. Check that line reports the JDK you intended before
trusting the diff — it is the only confirmation that the runtime was actually switched.
