/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.dissect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.mockito.internal.util.collections.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiAlphanumOfLengthBetween;

public class DissectParserTests extends ESTestCase {

    public void testJavaDocExamples() {
        assertMatch("%{a} %{b},%{c}", "foo bar,baz", Arrays.asList("a", "b", "c"), Arrays.asList("foo", "bar", "baz"));
        assertMiss("%{a},%{b}:%{c}", "foo,bar,baz");
        assertMatch("%{a->} %{b} %{c}", "foo         bar baz", Arrays.asList("a", "b", "c"), Arrays.asList("foo", "bar", "baz"));
        assertMatch("%{a} %{+a} %{+a}", "foo bar baz", Arrays.asList("a"), Arrays.asList("foobarbaz"));
        assertMatch("%{a} %{+a/2} %{+a/1}", "foo bar baz", Arrays.asList("a"), Arrays.asList("foobazbar"));
        assertMatch("%{*a} %{b} %{&a}", "foo bar baz", Arrays.asList("foo", "b"), Arrays.asList("baz", "bar"));
        assertMatch("%{a} %{} %{c}", "foo bar baz", Arrays.asList("a", "c"), Arrays.asList("foo", "baz"));
        assertMatch("%{a} %{?skipme} %{c}", "foo bar baz", Arrays.asList("a", "c"), Arrays.asList("foo", "baz"));
        assertMatch("%{a},%{b},%{c},%{d}", "foo,,,", Arrays.asList("a", "b", "c", "d"), Arrays.asList("foo", "", "", ""));
        assertMatch("%{a->},%{b}", "foo,,,,,,bar", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
    }

    /**
     * Borrowed from Logstash's test cases:
     * https://github.com/logstash-plugins/logstash-filter-dissect/blob/master/src/test/java/org/logstash/dissect/DissectorTest.java
     * Append Note - Logstash appends with the delimiter as the separator between values, this uses a user defined separator
     */
    public void testLogstashSpecs() {
        assertMatch("%{a} %{b->} %{c}", "foo bar   baz", Arrays.asList("a", "b", "c"), Arrays.asList("foo", "bar", "baz"));
        assertMiss("%{a}%{b} %{c}", null);
        assertMiss("%{a} %{b}%{c} %{d}", "foo bar baz");
        assertMiss("%{a} %{b} %{c}%{d}", "foo bar baz quux");
        assertMatch("%{a} %{b->} %{c}", "foo bar   baz", Arrays.asList("a", "b", "c"), Arrays.asList("foo", "bar", "baz"));
        assertMatch("%{a} %{} %{c}", "foo bar baz", Arrays.asList("a", "c"), Arrays.asList("foo", "baz"));
        assertMatch("%{a} %{b} %{+b} %{z}", "foo bar baz quux", Arrays.asList("a", "b", "z"), Arrays.asList("foo", "bar baz", "quux"), " ");
        assertMatch("%{a}------->%{b}", "foo------->bar baz quux", Arrays.asList("a", "b"), Arrays.asList("foo", "bar baz quux"));
        assertMatch("%{a}------->%{}", "foo------->bar baz quux", Arrays.asList("a"), Arrays.asList("foo"));
        assertMatch("%{a} » %{b}»%{c}€%{d}", "foo » bar»baz€quux",
            Arrays.asList("a", "b", "c", "d"), Arrays.asList("foo", "bar", "baz", "quux"));
        assertMatch("%{a} %{b} %{+a}", "foo bar baz quux", Arrays.asList("a", "b"), Arrays.asList("foo baz quux", "bar"), " ");
        //Logstash supports implicit ordering based anchored by the key without the '+'
        //This implementation will only honor implicit ordering for appending right to left else explicit order (/N) is required.
        //The results of this test differ from Logstash.
        assertMatch("%{+a} %{a} %{+a} %{b}", "December 31 1999 quux",
            Arrays.asList("a", "b"), Arrays.asList("December 31 1999", "quux"), " ");
        //Same test as above, but with same result as Logstash using explicit ordering in the pattern
        assertMatch("%{+a/1} %{a} %{+a/2} %{b}", "December 31 1999 quux",
            Arrays.asList("a", "b"), Arrays.asList("31 December 1999", "quux"), " ");
        assertMatch("%{+a/2} %{+a/4} %{+a/1} %{+a/3}", "bar quux foo baz", Arrays.asList("a"), Arrays.asList("foo bar baz quux"), " ");
        assertMatch("%{+a} %{b}", "foo bar", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
        assertMatch("%{+a} %{b} %{+a} %{c}", "foo bar baz quux",
            Arrays.asList("a", "b", "c"), Arrays.asList("foo baz", "bar", "quux"), " ");
        assertMatch("%{} %{syslog_timestamp} %{hostname} %{rt}: %{reason} %{+reason} %{src_ip}/%{src_port}->%{dst_ip}/%{dst_port} " +
                "%{polrt} %{+polrt} %{+polrt} %{from_zone} %{to_zone} %{rest}",
            "42 2016-05-25T14:47:23Z host.name.com RT_FLOW - RT_FLOW_SESSION_DENY: session denied 2.2.2.20/60000->1.1.1.10/8090 None " +
                "6(0) DEFAULT-DENY ZONE-UNTRUST ZONE-DMZ UNKNOWN UNKNOWN N/A(N/A) ge-0/0/0.0",
            Arrays.asList("syslog_timestamp", "hostname", "rt", "reason", "src_ip", "src_port", "dst_ip", "dst_port", "polrt"
                , "from_zone", "to_zone", "rest"),
            Arrays.asList("2016-05-25T14:47:23Z", "host.name.com", "RT_FLOW - RT_FLOW_SESSION_DENY", "session denied", "2.2.2.20", "60000"
                , "1.1.1.10", "8090", "None 6(0) DEFAULT-DENY", "ZONE-UNTRUST", "ZONE-DMZ", "UNKNOWN UNKNOWN N/A(N/A) ge-0/0/0.0"), " ");
        assertBadKey("%{+/2}");
        assertBadKey("%{&+a_field}");
        assertMatch("%{a->}   %{b->}---%{c}", "foo            bar------------baz",
            Arrays.asList("a", "b", "c"), Arrays.asList("foo", "bar", "baz"));
        assertMatch("%{->}-%{a}", "-----666", Arrays.asList("a"), Arrays.asList("666"));
        assertMatch("%{?skipme->}-%{a}", "-----666", Arrays.asList("a"), Arrays.asList("666"));
        assertMatch("%{a},%{b},%{c},%{d},%{e},%{f}", "111,,333,,555,666",
            Arrays.asList("a", "b", "c", "d", "e", "f"), Arrays.asList("111", "", "333", "", "555", "666"));
        assertMatch("%{a}.࿏.%{b}", "⟳༒.࿏.༒⟲", Arrays.asList("a", "b"), Arrays.asList("⟳༒", "༒⟲"));
        assertMatch("%{a}", "子", Arrays.asList("a"), Arrays.asList("子"));
        assertMatch("%{a}{\n}%{b}", "aaa{\n}bbb", Arrays.asList("a", "b"), Arrays.asList("aaa", "bbb"));
        assertMiss("MACHINE[%{a}] %{b}", "1234567890 MACHINE[foo] bar");
        assertMiss("%{a} %{b} %{c}", "foo:bar:baz");
        assertMatch("/var/%{key1}/log/%{key2}.log", "/var/foo/log/bar.log", Arrays.asList("key1", "key2"), Arrays.asList("foo", "bar"));
        assertMatch("%{a->}   %{b}-.-%{c}-%{d}-..-%{e}-%{f}-%{g}-%{h}", "foo            bar-.-baz-1111-..-22-333-4444-55555",
            Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h"),
            Arrays.asList("foo", "bar", "baz", "1111", "22", "333", "4444", "55555"));
    }

    public void testBasicMatch() {
        String valueFirstInput = "";
        String keyFirstPattern = "";
        String delimiterFirstInput = "";
        String delimiterFirstPattern = "";
        //parallel arrays
        List<String> expectedKeys = new ArrayList<>(Sets.newSet(generateRandomStringArray(100, 10, false, false)));
        List<String> expectedValues = new ArrayList<>(expectedKeys.size());
        for (String key : expectedKeys) {
            String value = randomAsciiAlphanumOfLengthBetween(1, 100);
            String delimiter = Integer.toString(randomInt()); //int to ensures values and delimiters don't overlap, else validation can fail
            keyFirstPattern += "%{" + key + "}" + delimiter;
            valueFirstInput += value + delimiter;
            delimiterFirstPattern += delimiter + "%{" + key + "}";
            delimiterFirstInput += delimiter + value;
            expectedValues.add(value);
        }
        assertMatch(keyFirstPattern, valueFirstInput, expectedKeys, expectedValues);
        assertMatch(delimiterFirstPattern, delimiterFirstInput, expectedKeys, expectedValues);
    }

    public void testBasicMatchUnicode() {
        String valueFirstInput = "";
        String keyFirstPattern = "";
        String delimiterFirstInput = "";
        String delimiterFirstPattern = "";
        //parallel arrays
        List<String> expectedKeys = new ArrayList<>();
        List<String> expectedValues = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 100); i++) {
            String key = randomAsciiAlphanumOfLengthBetween(1, 100);
            while (expectedKeys.contains(key)) { // keys should be unique in this test
                key = randomAsciiAlphanumOfLengthBetween(1, 100);
            }
            String value = randomRealisticUnicodeOfCodepointLengthBetween(1, 100);
            String delimiter = Integer.toString(randomInt()); //int to ensures values and delimiters don't overlap, else validation can fail
            keyFirstPattern += "%{" + key + "}" + delimiter;
            valueFirstInput += value + delimiter;
            delimiterFirstPattern += delimiter + "%{" + key + "}";
            delimiterFirstInput += delimiter + value;
            expectedKeys.add(key);
            expectedValues.add(value);
        }
        assertMatch(keyFirstPattern, valueFirstInput, expectedKeys, expectedValues);
        assertMatch(delimiterFirstPattern, delimiterFirstInput, expectedKeys, expectedValues);
    }

    public void testMatchUnicode() {
        assertMatch("%{a} %{b}", "foo 子", Arrays.asList("a", "b"), Arrays.asList("foo", "子"));
        assertMatch("%{a}࿏%{b} %{c}", "⟳༒࿏༒⟲ 子", Arrays.asList("a", "b", "c"), Arrays.asList("⟳༒", "༒⟲", "子"));
        assertMatch("%{a}࿏%{+a} %{+a}", "⟳༒࿏༒⟲ 子", Arrays.asList("a"), Arrays.asList("⟳༒༒⟲子"));
        assertMatch("%{a}࿏%{+a/2} %{+a/1}", "⟳༒࿏༒⟲ 子", Arrays.asList("a"), Arrays.asList("⟳༒子༒⟲"));
        assertMatch("%{a->}࿏%{b}", "⟳༒࿏࿏࿏࿏࿏༒⟲", Arrays.asList("a", "b"), Arrays.asList("⟳༒", "༒⟲"));
        assertMatch("%{*a}࿏%{&a}", "⟳༒࿏༒⟲", Arrays.asList("⟳༒"), Arrays.asList("༒⟲"));
        assertMatch("%{}࿏%{a}", "⟳༒࿏༒⟲", Arrays.asList("a"), Arrays.asList("༒⟲"));
    }

    public void testMatchRemainder() {
        assertMatch("%{a}", "foo bar the rest", Arrays.asList("a"), Arrays.asList("foo bar the rest"));
        assertMatch("%{a} %{b}", "foo bar the rest", Arrays.asList("a", "b"), Arrays.asList("foo", "bar the rest"));
        assertMatch("%{} %{b}", "foo bar the rest", Arrays.asList("b"), Arrays.asList("bar the rest"));
        assertMatch("%{a} %{b->}", "foo bar the rest", Arrays.asList("a", "b"), Arrays.asList("foo", "bar the rest"));
        assertMatch("%{*a} %{&a}", "foo bar the rest", Arrays.asList("foo"), Arrays.asList("bar the rest"));
        assertMatch("%{a} %{+a}", "foo bar the rest", Arrays.asList("a"), Arrays.asList("foo bar the rest"), " ");
    }

    public void testAppend() {
        assertMatch("%{a} %{+a} %{+a}", "foo bar baz", Arrays.asList("a"), Arrays.asList("foobarbaz"));
        assertMatch("%{a} %{+a} %{b} %{+b}", "foo bar baz lol", Arrays.asList("a", "b"), Arrays.asList("foobar", "bazlol"));
        assertMatch("%{a} %{+a/2} %{+a/1}", "foo bar baz", Arrays.asList("a"), Arrays.asList("foobazbar"));
        assertMatch("%{a} %{+a/2} %{+a/1}", "foo bar baz", Arrays.asList("a"), Arrays.asList("foo baz bar"), " ");
    }

    public void testAssociate() {
        assertMatch("%{*a} %{&a}", "foo bar", Arrays.asList("foo"), Arrays.asList("bar"));
        assertMatch("%{&a} %{*a}", "foo bar", Arrays.asList("bar"), Arrays.asList("foo"));
        assertMatch("%{*a} %{&a} %{*b} %{&b}", "foo bar baz lol", Arrays.asList("foo", "baz"), Arrays.asList("bar", "lol"));
        assertMatch("%{*a} %{&a} %{c} %{*b} %{&b}", "foo bar x baz lol",
            Arrays.asList("foo", "baz", "c"), Arrays.asList("bar", "lol", "x"));
        assertBadPattern("%{*a} %{a}");
        assertBadPattern("%{a} %{&a}");
        assertMiss("%{*a} %{&a} {a} %{*b} %{&b}", "foo bar x baz lol");
    }

    public void testAppendAndAssociate() {
        assertMatch("%{a} %{+a} %{*b} %{&b}", "foo bar baz lol", Arrays.asList("a", "baz"), Arrays.asList("foobar", "lol"));
        assertMatch("%{a->} %{+a/2} %{+a/1} %{*b} %{&b}", "foo      bar baz lol x",
            Arrays.asList("a", "lol"), Arrays.asList("foobazbar", "x"));
    }

    public void testEmptyKey() {
        assertMatch("%{} %{b}", "foo bar", Arrays.asList("b"), Arrays.asList("bar"));
        assertMatch("%{a} %{}", "foo bar", Arrays.asList("a"), Arrays.asList("foo"));
        assertMatch("%{->} %{b}", "foo        bar", Arrays.asList("b"), Arrays.asList("bar"));
        assertMatch("%{->} %{b}", "        bar", Arrays.asList("b"), Arrays.asList("bar"));
        assertMatch("%{a} %{->}", "foo  bar       ", Arrays.asList("a"), Arrays.asList("foo"));
    }

    public void testNamedSkipKey() {
        assertMatch("%{?foo} %{b}", "foo bar", Arrays.asList("b"), Arrays.asList("bar"));
        assertMatch("%{?} %{b}", "foo bar", Arrays.asList("b"), Arrays.asList("bar"));
        assertMatch("%{a} %{?bar}", "foo bar", Arrays.asList("a"), Arrays.asList("foo"));
        assertMatch("%{?foo->} %{b}", "foo        bar", Arrays.asList("b"), Arrays.asList("bar"));
        assertMatch("%{?->} %{b}", "foo        bar", Arrays.asList("b"), Arrays.asList("bar"));
        assertMatch("%{?foo->} %{b}", "        bar", Arrays.asList("b"), Arrays.asList("bar"));
        assertMatch("%{a} %{->?bar}", "foo  bar       ", Arrays.asList("a"), Arrays.asList("foo"));
        assertMatch("%{a} %{?skipme} %{?skipme}", "foo  bar  baz", Arrays.asList("a"), Arrays.asList("foo"));
        assertMatch("%{a} %{?} %{?}", "foo  bar  baz", Arrays.asList("a"), Arrays.asList("foo"));
    }

    public void testConsecutiveDelimiters() {
        //leading
        assertMatch("%{->},%{a}", ",,,,,foo", Arrays.asList("a"), Arrays.asList("foo"));
        assertMatch("%{a->},%{b}", ",,,,,foo", Arrays.asList("a", "b"), Arrays.asList("", "foo"));
        //trailing
        assertMatch("%{a->},", "foo,,,,,", Arrays.asList("a"), Arrays.asList("foo"));
        assertMatch("%{a} %{b},", "foo bar,,,,,", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
        assertMatch("%{a} %{b->},", "foo bar,,,,,", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
        //middle
        assertMatch("%{a->},%{b}", "foo,,,,,bar", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
        assertMatch("%{a->} %{b}", "foo     bar", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
        assertMatch("%{a->}x%{b}", "fooxxxxxbar", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
        assertMatch("%{a->} xyz%{b}", "foo xyz xyz xyz xyz xyzbar", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
        //skipped with empty values
        assertMatch("%{a},%{b},%{c},%{d}", "foo,,,", Arrays.asList("a", "b", "c", "d"), Arrays.asList("foo", "", "", ""));
        assertMatch("%{a},%{b},%{c},%{d}", "foo,,bar,baz", Arrays.asList("a", "b", "c", "d"), Arrays.asList("foo", "", "bar", "baz"));
        assertMatch("%{a},%{b},%{c},%{d}", "foo,,,baz", Arrays.asList("a", "b", "c", "d"), Arrays.asList("foo", "", "", "baz"));
        assertMatch("%{a},%{b},%{c},%{d}", ",bar,,baz", Arrays.asList("a", "b", "c", "d"), Arrays.asList("", "bar", "", "baz"));
        assertMatch("%{->},%{a->},%{b}", ",,,bar,,baz", Arrays.asList("a", "b"), Arrays.asList("bar", "baz"));
    }

    public void testAppendWithConsecutiveDelimiters() {
        assertMatch("%{+a/1},%{+a/3}-%{+a/2} %{b}", "foo,bar----baz lol", Arrays.asList("a", "b"), Arrays.asList("foobar", ""));
        assertMatch("%{+a/1},%{+a/3->}-%{+a/2} %{b}", "foo,bar----baz lol", Arrays.asList("a", "b"), Arrays.asList("foobazbar", "lol"));
    }

    public void testSkipRightPadding() {
        assertMatch("%{a->} %{b}", "foo bar", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
        assertMatch("%{a->} %{b}", "foo            bar", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
        assertMatch("%{->} %{a}", "foo            bar", Arrays.asList("a"), Arrays.asList("bar"));
        assertMatch("%{a->} %{+a->} %{*b->} %{&b->} %{c}", "foo       bar    baz  lol    x",
            Arrays.asList("a", "baz", "c"), Arrays.asList("foobar", "lol", "x"));
    }

    public void testTrimmedEnd() {
        assertMatch("%{a} %{b}", "foo bar", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
        assertMatch("%{a} %{b->} ", "foo bar        ", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
        //only whitespace is trimmed in the absence of trailing characters
        assertMatch("%{a} %{b->}", "foo bar,,,,,,", Arrays.asList("a", "b"), Arrays.asList("foo", "bar,,,,,,"));
        //consecutive delimiters + right padding can be used to skip over the trailing delimiters
        assertMatch("%{a} %{b->},", "foo bar,,,,,,", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
    }

    public void testLeadingDelimiter() {
        assertMatch(",,,%{a} %{b}", ",,,foo bar", Arrays.asList("a", "b"), Arrays.asList("foo", "bar"));
        assertMatch(",%{a} %{b}", ",,foo bar", Arrays.asList("a", "b"), Arrays.asList(",foo", "bar"));
    }

    /**
     * Runtime errors
     */
    public void testMiss() {
        assertMiss("%{a}%{b}", "foo");
        assertMiss("%{a},%{b}", "foo bar");
        assertMiss("%{a}, %{b}", "foo,bar");
        assertMiss("x%{a},%{b}", "foo,bar");
        assertMiss("x%{},%{b}", "foo,bar");
        assertMiss("leading_delimiter_long%{a}", "foo");
        assertMiss("%{a}trailing_delimiter_long", "foo");
        assertMiss("leading_delimiter_long%{a}trailing_delimiter_long", "foo");
        assertMiss("%{a}x", "foo");
        assertMiss("%{a},%{b}x", "foo,bar");
    }

    /**
     * Construction errors
     */
    public void testBadPatternOrKey() {
        assertBadPattern("");
        assertBadPattern("{}");
        assertBadPattern("%{*a} %{&b}");
        assertBadKey("%{*}");
        assertBadKey("%{++}");
    }

    public void testSyslog() {
        assertMatch("%{timestamp} %{+timestamp} %{+timestamp} %{logsource} %{program}[%{pid}]: %{message}",
            "Mar 16 00:01:25 evita postfix/smtpd[1713]: connect from camomile.cloud9.net[168.100.1.3]",
            Arrays.asList("timestamp", "logsource", "program", "pid", "message"),
            Arrays.asList("Mar 16 00:01:25", "evita", "postfix/smtpd", "1713", "connect from camomile.cloud9.net[168.100.1.3]"), " ");
    }

    public void testApacheLog() {
        assertMatch("%{clientip} %{ident} %{auth} [%{timestamp}] \"%{verb} %{request} HTTP/%{httpversion}\" %{response} %{bytes}" +
                " \"%{referrer}\" \"%{agent}\" %{->}",
            "31.184.238.164 - - [24/Jul/2014:05:35:37 +0530] \"GET /logs/access.log HTTP/1.0\" 200 69849 " +
                "\"http://8rursodiol.enjin.com\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) " +
                "Chrome/30.0.1599.12785 YaBrowser/13.12.1599.12785 Safari/537.36\" \"www.dlwindianrailways.com\"",
            Arrays.asList("clientip", "ident", "auth", "timestamp", "verb", "request", "httpversion", "response", "bytes",
                "referrer", "agent"),
            Arrays.asList("31.184.238.164", "-", "-", "24/Jul/2014:05:35:37 +0530", "GET", "/logs/access.log", "1.0", "200", "69849",
                "http://8rursodiol.enjin.com", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36" +
                    " (KHTML, like Gecko) Chrome/30.0.1599.12785 YaBrowser/13.12.1599.12785 Safari/537.36"));
    }

    /**
     * Shared specification between Beats, Logstash, and Ingest node
     */
    public void testJsonSpecification() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(this.getClass().getResourceAsStream("/specification/tests.json"));
        Iterator<JsonNode> tests = rootNode.elements();
        while (tests.hasNext()) {
            JsonNode test = tests.next();
            boolean skip = test.path("skip").asBoolean();
            if (!skip) {
                String name = test.path("name").asText();
                logger.debug("Running Json specification: " + name);
                String pattern = test.path("tok").asText();
                String input = test.path("msg").asText();
                String append = test.path("append").asText();
                boolean fail = test.path("fail").asBoolean();
                Iterator<Map.Entry<String, JsonNode>> expected = test.path("expected").fields();
                List<String> expectedKeys = new ArrayList<>();
                List<String> expectedValues = new ArrayList<>();
                expected.forEachRemaining(entry -> {
                    expectedKeys.add(entry.getKey());
                    expectedValues.add(entry.getValue().asText());
                });
                if (fail) {
                    assertFail(pattern, input);
                } else {
                    assertMatch(pattern, input, expectedKeys, expectedValues, append);
                }
            }
        }
    }

    private DissectException assertFail(String pattern, String input){
        return expectThrows(DissectException.class, () -> new DissectParser(pattern, null).parse(input));
    }

    private void assertMiss(String pattern, String input) {
        DissectException e = assertFail(pattern, input);
        assertThat(e.getMessage(), CoreMatchers.containsString("Unable to find match for dissect pattern"));
        assertThat(e.getMessage(), CoreMatchers.containsString(pattern));
        assertThat(e.getMessage(), input == null ? CoreMatchers.containsString("null") : CoreMatchers.containsString(input));
    }

    private void assertBadPattern(String pattern) {
        DissectException e = assertFail(pattern, null);
        assertThat(e.getMessage(), CoreMatchers.containsString("Unable to parse pattern"));
        assertThat(e.getMessage(), CoreMatchers.containsString(pattern));
    }

    private void assertBadKey(String pattern, String key) {
        DissectException e = assertFail(pattern, null);
        assertThat(e.getMessage(), CoreMatchers.containsString("Unable to parse key"));
        assertThat(e.getMessage(), CoreMatchers.containsString(key));
    }

    private void assertBadKey(String pattern) {
        assertBadKey(pattern, pattern.replace("%{", "").replace("}", ""));
    }

    private void assertMatch(String pattern, String input, List<String> expectedKeys, List<String> expectedValues) {
        assertMatch(pattern, input, expectedKeys, expectedValues, null);
    }

    private void assertMatch(String pattern, String input, List<String> expectedKeys, List<String> expectedValues, String appendSeperator) {
        Map<String, String> results = new DissectParser(pattern, appendSeperator).parse(input);
        assertThat(results.size(), Matchers.equalTo(expectedKeys.size()));
        assertThat(results.size(), Matchers.equalTo(expectedValues.size()));
        for (int i = 0; i < results.size(); i++) {
            final String key = expectedKeys.get(i);
            assertThat(results.get(key), Matchers.equalTo(expectedValues.get(i)));
        }
    }
}
