/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.util.HashMap;
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

    public void testDecodeQueryString() {
        Map<String, String> params = new HashMap<>();

        String uri = "something?test=value";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(1));
        assertThat(params.get("test"), equalTo("value"));

        params.clear();
        uri = Strings.format("something?test=value%ctest1=value1", randomDelimiter());
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(2));
        assertThat(params.get("test"), equalTo("value"));
        assertThat(params.get("test1"), equalTo("value1"));

        params.clear();
        uri = "something";
        RestUtils.decodeQueryString(uri, uri.length(), params);
        assertThat(params.size(), equalTo(0));

        params.clear();
        uri = "something";
        RestUtils.decodeQueryString(uri, -1, params);
        assertThat(params.size(), equalTo(0));
    }

    public void testDecodeQueryStringEdgeCases() {
        Map<String, String> params = new HashMap<>();

        String uri = "something?";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(0));

        params.clear();
        uri = Strings.format("something?%c", randomDelimiter());
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(0));

        params.clear();
        uri = Strings.format("something?p=v%c%cp1=v1", randomDelimiter(), randomDelimiter());
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(2));
        assertThat(params.get("p"), equalTo("v"));
        assertThat(params.get("p1"), equalTo("v1"));

        params.clear();
        uri = "something?=";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(0));

        params.clear();
        uri = Strings.format("something?%c=", randomDelimiter());
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(0));

        params.clear();
        uri = "something?a";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(1));
        assertThat(params.get("a"), equalTo(""));

        params.clear();
        uri = Strings.format("something?p=v%ca", randomDelimiter());
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(2));
        assertThat(params.get("a"), equalTo(""));
        assertThat(params.get("p"), equalTo("v"));

        params.clear();
        uri = Strings.format("something?p=v%ca%cp1=v1", randomDelimiter(), randomDelimiter());
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(3));
        assertThat(params.get("a"), equalTo(""));
        assertThat(params.get("p"), equalTo("v"));
        assertThat(params.get("p1"), equalTo("v1"));

        params.clear();
        uri = Strings.format("something?p=v%ca%cb%cp1=v1", randomDelimiter(), randomDelimiter(), randomDelimiter());
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(4));
        assertThat(params.get("a"), equalTo(""));
        assertThat(params.get("b"), equalTo(""));
        assertThat(params.get("p"), equalTo("v"));
        assertThat(params.get("p1"), equalTo("v1"));
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
        Map<String, String> params = new HashMap<>();

        // This is a valid URL
        String uri = String.format(
            Locale.ROOT,
            host + "/:@-._~!$%c'()*+,=;:@-._~!$%c'()*+,=:@-._~!$%c'()*+,==?/?:@-._~!$'()*+,=/?:@-._~!$'()*+,==#/?:@-._~!$%c'()*+,;=",
            randomDelimiter(),
            randomDelimiter(),
            randomDelimiter(),
            randomDelimiter()
        );
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.get("/?:@-._~!$'()* ,"), equalTo("/?:@-._~!$'()* ,=="));
        assertThat(params.size(), equalTo(1));
    }

    public void testReservedParameters() {
        for (var reservedParam : INTERNAL_MARKER_REQUEST_PARAMETERS) {
            Map<String, String> params = new HashMap<>();
            String uri = "something?" + reservedParam + "=value";
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params)
            );
            assertEquals(exception.getMessage(), "parameter [" + reservedParam + "] is reserved and may not be set");
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
