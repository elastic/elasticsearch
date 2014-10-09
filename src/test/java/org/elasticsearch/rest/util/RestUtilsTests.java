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

package org.elasticsearch.rest.util;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.rest.support.RestUtils;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.collect.Maps.newHashMap;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class RestUtilsTests extends ElasticsearchTestCase {

    @Test
    public void testDecodeQueryString() {
        Map<String, String> params = newHashMap();

        String uri = "something?test=value";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(1));
        assertThat(params.get("test"), equalTo("value"));

        params.clear();
        uri = "something?test=value&test1=value1";
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

    @Test
    public void testDecodeQueryStringEdgeCases() {
        Map<String, String> params = newHashMap();

        String uri = "something?";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(0));

        params.clear();
        uri = "something?&";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(0));

        params.clear();
        uri = "something?p=v&&p1=v1";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(2));
        assertThat(params.get("p"), equalTo("v"));
        assertThat(params.get("p1"), equalTo("v1"));

        params.clear();
        uri = "something?=";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(0));

        params.clear();
        uri = "something?&=";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(0));

        params.clear();
        uri = "something?a";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(1));
        assertThat(params.get("a"), equalTo(""));

        params.clear();
        uri = "something?p=v&a";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(2));
        assertThat(params.get("a"), equalTo(""));
        assertThat(params.get("p"), equalTo("v"));

        params.clear();
        uri = "something?p=v&a&p1=v1";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(3));
        assertThat(params.get("a"), equalTo(""));
        assertThat(params.get("p"), equalTo("v"));
        assertThat(params.get("p1"), equalTo("v1"));

        params.clear();
        uri = "something?p=v&a&b&p1=v1";
        RestUtils.decodeQueryString(uri, uri.indexOf('?') + 1, params);
        assertThat(params.size(), equalTo(4));
        assertThat(params.get("a"), equalTo(""));
        assertThat(params.get("b"), equalTo(""));
        assertThat(params.get("p"), equalTo("v"));
        assertThat(params.get("p1"), equalTo("v1"));
    }

    @Test
    public void testCorsSettingIsARegex() {
        assertCorsSettingRegex("/foo/", Pattern.compile("foo"));
        assertCorsSettingRegex("/.*/", Pattern.compile(".*"));
        assertCorsSettingRegex("/https?:\\/\\/localhost(:[0-9]+)?/", Pattern.compile("https?:\\/\\/localhost(:[0-9]+)?"));
        assertCorsSettingRegexMatches("/https?:\\/\\/localhost(:[0-9]+)?/", true, "http://localhost:9200", "http://localhost:9215", "https://localhost:9200", "https://localhost");
        assertCorsSettingRegexMatches("/https?:\\/\\/localhost(:[0-9]+)?/", false, "htt://localhost:9200", "http://localhost:9215/foo", "localhost:9215");
        assertCorsSettingRegexIsNull("//");
        assertCorsSettingRegexIsNull("/");
        assertCorsSettingRegexIsNull("/foo");
        assertCorsSettingRegexIsNull("foo");
        assertCorsSettingRegexIsNull("");
        assertThat(RestUtils.getCorsSettingRegex(ImmutableSettings.EMPTY), is(nullValue()));
    }

    private void assertCorsSettingRegexIsNull(String settingsValue) {
        assertThat(RestUtils.getCorsSettingRegex(settingsBuilder().put("http.cors.allow-origin", settingsValue).build()), is(nullValue()));
    }

    private void assertCorsSettingRegex(String settingsValue, Pattern pattern) {
        assertThat(RestUtils.getCorsSettingRegex(settingsBuilder().put("http.cors.allow-origin", settingsValue).build()).toString(), is(pattern.toString()));
    }

    private void assertCorsSettingRegexMatches(String settingsValue, boolean expectMatch, String ... candidates) {
        Pattern pattern = RestUtils.getCorsSettingRegex(settingsBuilder().put("http.cors.allow-origin", settingsValue).build());
        for (String candidate : candidates) {
            assertThat(String.format(Locale.ROOT, "Expected pattern %s to match against %s: %s", settingsValue, candidate, expectMatch),
                    pattern.matcher(candidate).matches(), is(expectMatch));
        }
    }
}
