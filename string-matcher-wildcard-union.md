# StringMatcher rejects valid wildcard patterns when combined

Elasticsearch 9.7.0-SNAPSHOT, revision `5114700401fa`. In X-Pack core, `StringMatcher.of("apps/*")` and `StringMatcher.of("*/nightly")` each accept their pattern. Passing the same two patterns to one matcher throws an exception during construction.

## Reproduction

Add this method to `org.elasticsearch.xpack.core.security.support.StringMatcherTests` and run the Gradle command below.

```java
public void testUnionOfPrefixAndPathSuffixWildcards() {
    assertTrue(StringMatcher.of("apps/*").test("apps/daily"));
    assertTrue(StringMatcher.of("*/nightly").test("jobs/nightly"));

    StringMatcher combined = StringMatcher.of("apps/*", "*/nightly");
    assertTrue(combined.test("apps/daily"));
    assertTrue(combined.test("jobs/nightly"));
    assertFalse(combined.test("jobs/daily"));
}
```

```sh
./gradlew :x-pack:plugin:core:test --tests 'org.elasticsearch.xpack.core.security.support.StringMatcherTests.testUnionOfPrefixAndPathSuffixWildcards'
```

**Expected behavior.** The combined matcher accepts a string when either individual matcher accepts it.

**Observed behavior.** The individual matchers work. Constructing `combined` throws `ElasticsearchSecurityException`. Its cause is an `IllegalArgumentException` reporting `invalid pattern [/nightly]`. The input supplied by the caller was `*/nightly`.

## Cause

The multi-pattern optimization in `Automatons.buildAutomaton(Collection)` strips the leading `*` so it can compile suffixes together. It passes the remaining `/nightly` through the top-level pattern parser, where a leading slash denotes a regular expression and requires a closing slash. The single-pattern path compiles the original wildcard directly. The fix should preserve wildcard syntax when compiling the stripped suffix. [Suffix grouping and compilation](https://github.com/elastic/elasticsearch/blob/5114700401fa32cb7b316283a0df22d7ca0f320b/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/security/support/Automatons.java#L195-L234) · [Pattern parser](https://github.com/elastic/elasticsearch/blob/5114700401fa32cb7b316283a0df22d7ca0f320b/x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/security/support/Automatons.java#L284-L304)
