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

package org.elasticsearch.rest.action.support;

import org.elasticsearch.common.Table;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.action.support.RestTable.buildDisplayHeaders;
import static org.elasticsearch.rest.action.support.RestTable.buildResponse;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class RestTableTests extends ESTestCase {

    private static final String APPLICATION_JSON = XContentType.JSON.mediaType();
    private static final String APPLICATION_YAML = XContentType.YAML.mediaType();
    private static final String APPLICATION_SMILE = XContentType.SMILE.mediaType();
    private static final String APPLICATION_CBOR = XContentType.CBOR.mediaType();
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String ACCEPT = "Accept";
    private static final String TEXT_PLAIN = "text/plain; charset=UTF-8";
    private static final String TEXT_TABLE_BODY = "foo foo foo foo foo foo foo foo\n";
    private static final String JSON_TABLE_BODY = "[{\"bulk.foo\":\"foo\",\"bulk.bar\":\"foo\",\"aliasedBulk\":\"foo\"," +
            "\"aliasedSecondBulk\":\"foo\",\"unmatched\":\"foo\"," +
            "\"invalidAliasesBulk\":\"foo\",\"timestamp\":\"foo\",\"epoch\":\"foo\"}]";
    private static final String YAML_TABLE_BODY = "---\n" +
            "- bulk.foo: \"foo\"\n" +
            "  bulk.bar: \"foo\"\n" +
            "  aliasedBulk: \"foo\"\n" +
            "  aliasedSecondBulk: \"foo\"\n" +
            "  unmatched: \"foo\"\n" +
            "  invalidAliasesBulk: \"foo\"\n" +
            "  timestamp: \"foo\"\n" +
            "  epoch: \"foo\"\n";
    private Table table = new Table();
    private FakeRestRequest restRequest = new FakeRestRequest();

    @Before
    public void setup() {
        table.startHeaders();
        table.addCell("bulk.foo", "alias:f;desc:foo");
        table.addCell("bulk.bar", "alias:b;desc:bar");
        // should be matched as well due to the aliases
        table.addCell("aliasedBulk", "alias:bulkWhatever;desc:bar");
        table.addCell("aliasedSecondBulk", "alias:foobar,bulkolicious,bulkotastic;desc:bar");
        // no match
        table.addCell("unmatched", "alias:un.matched;desc:bar");
        // invalid alias
        table.addCell("invalidAliasesBulk", "alias:,,,;desc:bar");
        // timestamp
        table.addCell("timestamp", "alias:ts");
        table.addCell("epoch", "alias:t");
        table.endHeaders();
    }

    public void testThatDisplayHeadersSupportWildcards() throws Exception {
        restRequest.params().put("h", "bulk*");
        List<RestTable.DisplayHeader> headers = buildDisplayHeaders(table, restRequest);

        List<String> headerNames = getHeaderNames(headers);
        assertThat(headerNames, contains("bulk.foo", "bulk.bar", "aliasedBulk", "aliasedSecondBulk"));
        assertThat(headerNames, not(hasItem("unmatched")));
    }

    public void testThatDisplayHeadersAreNotAddedTwice() throws Exception {
        restRequest.params().put("h", "nonexistent,bulk*,bul*");
        List<RestTable.DisplayHeader> headers = buildDisplayHeaders(table, restRequest);

        List<String> headerNames = getHeaderNames(headers);
        assertThat(headerNames, contains("bulk.foo", "bulk.bar", "aliasedBulk", "aliasedSecondBulk"));
        assertThat(headerNames, not(hasItem("unmatched")));
    }

    public void testThatWeUseTheAcceptHeaderJson() throws Exception {
        assertResponse(Collections.singletonMap(ACCEPT, APPLICATION_JSON),
                APPLICATION_JSON,
                JSON_TABLE_BODY);
    }

    public void testThatWeUseTheAcceptHeaderYaml() throws Exception {
        assertResponse(Collections.singletonMap(ACCEPT, APPLICATION_YAML),
                APPLICATION_YAML,
                YAML_TABLE_BODY);
    }

    public void testThatWeUseTheAcceptHeaderSmile() throws Exception {
        assertResponseContentType(Collections.singletonMap(ACCEPT, APPLICATION_SMILE),
                APPLICATION_SMILE);
    }

    public void testThatWeUseTheAcceptHeaderCbor() throws Exception {
        assertResponseContentType(Collections.singletonMap(ACCEPT, APPLICATION_CBOR),
                APPLICATION_CBOR);
    }

    public void testThatWeUseTheAcceptHeaderText() throws Exception {
        assertResponse(Collections.singletonMap(ACCEPT, TEXT_PLAIN),
                TEXT_PLAIN,
                TEXT_TABLE_BODY);
    }

    public void testIgnoreContentType() throws Exception {
        assertResponse(Collections.singletonMap(CONTENT_TYPE, APPLICATION_JSON),
                TEXT_PLAIN,
                TEXT_TABLE_BODY);
    }

    public void testThatDisplayHeadersWithoutTimestamp() throws Exception {
        restRequest.params().put("h", "timestamp,epoch,bulk*");
        restRequest.params().put("ts", "0");
        List<RestTable.DisplayHeader> headers = buildDisplayHeaders(table, restRequest);

        List<String> headerNames = getHeaderNames(headers);
        assertThat(headerNames, contains("bulk.foo", "bulk.bar", "aliasedBulk", "aliasedSecondBulk"));
        assertThat(headerNames, not(hasItem("timestamp")));
        assertThat(headerNames, not(hasItem("epoch")));
    }

    private RestResponse assertResponseContentType(Map<String, String> headers, String mediaType) throws Exception {
        FakeRestRequest requestWithAcceptHeader = new FakeRestRequest.Builder().withHeaders(headers).build();
        table.startRow();
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.endRow();
        RestResponse response = buildResponse(table, new AbstractRestChannel(requestWithAcceptHeader, true) {
            @Override
            public void sendResponse(RestResponse response) {
            }
        });

        assertThat(response.contentType(), equalTo(mediaType));
        return response;
    }

    private void assertResponse(Map<String, String> headers, String mediaType, String body) throws Exception {
        RestResponse response = assertResponseContentType(headers, mediaType);
        assertThat(response.content().toUtf8(), equalTo(body));
    }

    private List<String> getHeaderNames(List<RestTable.DisplayHeader> headers) {
        List<String> headerNames = new ArrayList<>();
        for (RestTable.DisplayHeader header : headers) {
            headerNames.add(header.name);
        }

        return headerNames;
    }
}
