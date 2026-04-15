/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.rest.RestRequest.INTERNAL_MARKER_REQUEST_PARAMETERS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RestUtilsTests extends ESTestCase {

    static char randomDelimiter() {
        return randomBoolean() ? '&' : ';';
    }

    public void testDecodeQueryStringFromUri() {
        var params = RequestParams.fromUri("something?test=value");
        assertThat(params.size(), equalTo(1));
        assertThat(params.get("test"), equalTo("value"));

        params = RequestParams.fromUri(Strings.format("something?test=value%ctest1=value1", randomDelimiter()));
        assertThat(params.size(), equalTo(2));
        assertThat(params.get("test"), equalTo("value"));
        assertThat(params.get("test1"), equalTo("value1"));

        // no query string
        assertThat(RequestParams.fromUri("something").isEmpty(), is(true));
    }

    public void testDecodeQueryStringFromUriEdgeCases() {
        // empty query string
        assertThat(RequestParams.fromUri("something?").size(), equalTo(0));

        assertThat(RequestParams.fromUri(Strings.format("something?%c", randomDelimiter())).size(), equalTo(0));

        var params = RequestParams.fromUri(Strings.format("something?p=v%c%cp1=v1", randomDelimiter(), randomDelimiter()));
        assertThat(params.size(), equalTo(2));
        assertThat(params.get("p"), equalTo("v"));
        assertThat(params.get("p1"), equalTo("v1"));

        assertThat(RequestParams.fromUri("something?=").size(), equalTo(0));

        assertThat(RequestParams.fromUri(Strings.format("something?%c=", randomDelimiter())).size(), equalTo(0));

        params = RequestParams.fromUri("something?a");
        assertThat(params.size(), equalTo(1));
        assertThat(params.get("a"), equalTo(""));

        params = RequestParams.fromUri(Strings.format("something?p=v%ca", randomDelimiter()));
        assertThat(params.size(), equalTo(2));
        assertThat(params.get("a"), equalTo(""));
        assertThat(params.get("p"), equalTo("v"));

        params = RequestParams.fromUri(Strings.format("something?p=v%ca%cp1=v1", randomDelimiter(), randomDelimiter()));
        assertThat(params.size(), equalTo(3));
        assertThat(params.get("a"), equalTo(""));
        assertThat(params.get("p"), equalTo("v"));
        assertThat(params.get("p1"), equalTo("v1"));

        params = RequestParams.fromUri(
            Strings.format("something?p=v%ca%cb%cp1=v1", randomDelimiter(), randomDelimiter(), randomDelimiter())
        );
        assertThat(params.size(), equalTo(4));
        assertThat(params.get("a"), equalTo(""));
        assertThat(params.get("b"), equalTo(""));
        assertThat(params.get("p"), equalTo("v"));
        assertThat(params.get("p1"), equalTo("v1"));
    }

    public void testDecodeQueryString() {
        var params = RequestParams.fromQueryString("test=value");
        assertThat(params.size(), equalTo(1));
        assertThat(params.getAll("test"), equalTo(List.of("value")));
    }

    public void testDecodeQueryStringMultipleValues() {
        var params = RequestParams.fromQueryString("match%5B%5D=up&match%5B%5D=http_requests_total&start=1609746000");
        assertThat(params.getAll("match[]"), equalTo(List.of("up", "http_requests_total")));
        assertThat(params.getAll("start"), equalTo(List.of("1609746000")));
    }

    public void testDecodeQueryStringMultipleValuesUnadorned() {
        var params = RequestParams.fromQueryString("match=up&match=http_requests_total&start=1609746000");
        assertThat(params.getAll("match"), equalTo(List.of("up", "http_requests_total")));
        assertThat(params.getAll("start"), equalTo(List.of("1609746000")));
    }

    public void testDecodeQueryStringDelimiters() {
        var params = RequestParams.fromQueryString(Strings.format("a=1%cb=2", randomDelimiter()));
        assertThat(params.getAll("a"), equalTo(List.of("1")));
        assertThat(params.getAll("b"), equalTo(List.of("2")));
    }

    public void testDecodeQueryStringEdgeCases() {
        // empty query string
        assertThat(RequestParams.fromQueryString("").isEmpty(), is(true));

        // key with no value
        assertThat(RequestParams.fromQueryString("a").getAll("a"), equalTo(List.of("")));
    }

    public void testDecodeQueryStringFragment() {
        // fragment should be excluded
        var params = RequestParams.fromUri("something?a=1#fragment");
        assertThat(params.getAll("a"), equalTo(List.of("1")));
        assertThat(params.containsKey("fragment"), is(false));
    }

    public void testDecodeQueryStringUrlEncoded() {
        var params = RequestParams.fromQueryString("match%5B%5D=up%7Bjob%3D%22prometheus%22%7D");
        assertThat(params.getAll("match[]"), equalTo(List.of("up{job=\"prometheus\"}")));
    }

    public void testCorsSettingIsARegex() {
        assertCorsSettingRegex("/foo/", Pattern.compile("foo"));
        assertCorsSettingRegex("/.*/", Pattern.compile(".*"));
        assertCorsSettingRegex("/https?:\\/\\/localhost(:[0-9]+)?/", Pattern.compile("https?:\\/\\/localhost(:[0-9]+)?"));
        assertCorsSettingRegexMatches(
            "/https?:\\/\\/localhost(:[0-9]+)?/",
            true,
            "http://localhost:9200",
            "http://localhost:9215",
            "https://localhost:9200",
            "https://localhost"
        );
        assertCorsSettingRegexMatches(
            "/https?:\\/\\/localhost(:[0-9]+)?/",
            false,
            "htt://localhost:9200",
            "http://localhost:9215/foo",
            "localhost:9215"
        );
        assertCorsSettingRegexIsNull("//");
        assertCorsSettingRegexIsNull("/");
        assertCorsSettingRegexIsNull("/foo");
        assertCorsSettingRegexIsNull("foo");
        assertCorsSettingRegexIsNull("");
    }

    public void testCrazyURL() {
        String host = "example.com";

        // This is a valid URL
        String uri = String.format(
            Locale.ROOT,
            host + "/:@-._~!$%c'()*+,=;:@-._~!$%c'()*+,=:@-._~!$%c'()*+,==?/?:@-._~!$'()*+,=/?:@-._~!$'()*+,==#/?:@-._~!$%c'()*+,;=",
            randomDelimiter(),
            randomDelimiter(),
            randomDelimiter(),
            randomDelimiter()
        );
        var params = RequestParams.fromUri(uri);
        assertThat(params.get("/?:@-._~!$'()* ,"), equalTo("/?:@-._~!$'()* ,=="));
        assertThat(params.size(), equalTo(1));
    }

    public void testReservedParameters() {
        for (var reservedParam : INTERNAL_MARKER_REQUEST_PARAMETERS) {
            RestRequest.BadParameterException exception = expectThrows(
                RestRequest.BadParameterException.class,
                () -> RequestParams.fromUri("something?" + reservedParam + "=value")
            );
            assertEquals(exception.getCause().getMessage(), "parameter [" + reservedParam + "] is reserved and may not be set");
        }
    }

    private void assertCorsSettingRegexIsNull(String settingsValue) {
        assertThat(RestUtils.checkCorsSettingForRegex(settingsValue), is(nullValue()));
    }

    private void assertCorsSettingRegex(String settingsValue, Pattern pattern) {
        assertThat(RestUtils.checkCorsSettingForRegex(settingsValue).toString(), is(pattern.toString()));
    }

    private void assertCorsSettingRegexMatches(String settingsValue, boolean expectMatch, String... candidates) {
        Pattern pattern = RestUtils.checkCorsSettingForRegex(settingsValue);
        for (String candidate : candidates) {
            assertThat(
                Strings.format("Expected pattern %s to match against %s: %s", settingsValue, candidate, expectMatch),
                pattern.matcher(candidate).matches(),
                is(expectMatch)
            );
        }
    }

    public void testGetMasterNodeTimeout() {
        assertEquals(
            TimeValue.timeValueSeconds(30),
            RestUtils.getMasterNodeTimeout(new FakeRestRequest.Builder(xContentRegistry()).build())
        );

        final var timeout = randomTimeValue();
        assertEquals(
            timeout,
            RestUtils.getMasterNodeTimeout(
                new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("master_timeout", timeout.getStringRep())).build()
            )
        );
    }

    public void testGetTimeout() {
        assertNull(RestUtils.getTimeout(new FakeRestRequest.Builder(xContentRegistry()).build()));

        final var timeout = randomTimeValue();
        assertEquals(
            timeout,
            RestUtils.getTimeout(
                new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("timeout", timeout.getStringRep())).build()
            )
        );
    }

    public void testGetAckTimeout() {
        assertEquals(TimeValue.timeValueSeconds(30), RestUtils.getAckTimeout(new FakeRestRequest.Builder(xContentRegistry()).build()));

        final var timeout = randomTimeValue();
        assertEquals(
            timeout,
            RestUtils.getAckTimeout(
                new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("timeout", timeout.getStringRep())).build()
            )
        );
    }
}
