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

package org.elasticsearch.search.scroll;

import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class RestSearchScrollActionTests extends ESTestCase {
    public void testParseSearchScrollRequest() throws Exception {
        XContentParser content = createParser(XContentFactory.jsonBuilder()
            .startObject()
                .field("scroll_id", "SCROLL_ID")
                .field("scroll", "1m")
            .endObject());

        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        RestSearchScrollAction.buildFromContent(content, searchScrollRequest);

        assertThat(searchScrollRequest.scrollId(), equalTo("SCROLL_ID"));
        assertThat(searchScrollRequest.scroll().keepAlive(), equalTo(TimeValue.parseTimeValue("1m", null, "scroll")));
    }

    public void testParseSearchScrollRequestWithInvalidJsonThrowsException() throws Exception {
        RestSearchScrollAction action = new RestSearchScrollAction(Settings.EMPTY, mock(RestController.class));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray("{invalid_json}"), XContentType.JSON).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse request body"));
    }

    public void testParseSearchScrollRequestWithUnknownParamThrowsException() throws Exception {
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        XContentParser invalidContent = createParser(XContentFactory.jsonBuilder()
                .startObject()
                    .field("scroll_id", "value_2")
                    .field("unknown", "keyword")
                .endObject());

        Exception e = expectThrows(IllegalArgumentException.class,
                () -> RestSearchScrollAction.buildFromContent(invalidContent, searchScrollRequest));
        assertThat(e.getMessage(), startsWith("Unknown parameter [unknown]"));
    }
}
