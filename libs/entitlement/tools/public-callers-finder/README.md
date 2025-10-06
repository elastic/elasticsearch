This tool scans the JDK on which it is running. It takes a list of methods (compatible with the output of the `securitymanager-scanner` and `jdk-api-extractor` tools),
and looks for the "public surface" of these methods (i.e. any class/method accessible from regular Java code that calls into the original list, directly or transitively).

It acts basically as a recursive "Find Usages" in Intellij, stopping at the first fully accessible point (public method on a public class).
The tool scans every method in every class inside the same java module; e.g.
if you have a private method `File#normalizedList`, it will scan `java.base` to find
public methods like `File#list(String)`, `File#list(FilenameFilter, String)` and
`File#listFiles(File)`.

The tool considers implemented interfaces (directly); e.g. if we're looking at a
method `C.m`, where `C implements I`, it will look for calls to `I.m`. It will
also consider (indirectly) calls to `S.m` (where `S` is a supertype of `C`), as
it treats calls to `super` in `S.m` as regular calls (e.g. `example() -> S.m() -> C.m()`).


In order to run the tool, use:
```shell
./gradlew :libs:entitlement:tools:public-callers-finder:run -Druntime.java=25 --args="<input-file> [<bubble-up-from-public>]" [-Des.entitlements.dump="<entitlements-dump-file>"]
```

- `input-file` is a `TAB`-separated TSV file containing the following columns:
  1. Module name
  2. unused
  3. unused
  4. Fully qualified class name (ASM style, with `/` separators)
  5. Method name
  6. Method descriptor (ASM signature)
  7. Visibility (PUBLIC/PUBLIC-METHOD/PRIVATE)


- optional: `bubble-up-from-public` is a boolean (`true|false`, default is `false`) indicating if the code should stop at the first public method or continue to find usages recursively even after reaching the "public surface".

- optional: if `entitlements-dump-file` is provided, all instrumented methods will be dumped to that file and checked against the output of this tool to mark methods as `COVERED` or `MISSING` (see below).

Examples:
```bash
./gradlew :libs:entitlement:tools:public-callers-finder:run --args="$PWD/sensitive-methods.tsv true" -Des.entitlements.dump="$PWD/entitlements.tsv"
```


The tool writes the following `TAB`-separated columns to standard out:

1. Module name
2. File name (from source root)
3. Line number
4. Fully qualified class name (ASM style, with `/` separators)
5. Method name
6. Method descriptor (ASM signature)
7. Visibility (PUBLIC/PUBLIC-METHOD/PRIVATE)
8. Optional: `COVERED` if method is instrumented with entitlement checks, `MISSING` otherwise
9. Original caller Module name
10. Original caller Class name (ASM style, with `/` separators)
11. Original caller Method name
12. Original caller Visibility

Example output:
```
java.base	DeleteOnExitHook.java	50	java/io/DeleteOnExitHook$1	run	()V	PUBLIC	java.base	java/io/File	delete	PUBLIC
java.base	ZipFile.java	254	java/util/zip/ZipFile	<init>	(Ljava/io/File;ILjava/nio/charset/Charset;)V	PUBLIC	java.base	java/io/File	delete	PUBLIC
java.logging	FileHandler.java	279	java/util/logging/FileHandler	<init>	()V	PUBLIC	java.base	java/io/File	delete	PUBLIC
```
