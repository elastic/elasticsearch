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

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestClearScrollAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class RestClearScrollActionTests extends ESTestCase {
    public void testParseClearScrollRequest() throws Exception {
        XContentParser content = createParser(XContentFactory.jsonBuilder()
                .startObject()
                    .array("scroll_id", "value_1", "value_2")
                .endObject());
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        RestClearScrollAction.buildFromContent(content, clearScrollRequest);
        assertThat(clearScrollRequest.scrollIds(), contains("value_1", "value_2"));
    }

    public void testParseClearScrollRequestWithInvalidJsonThrowsException() throws Exception {
        RestClearScrollAction action = new RestClearScrollAction(Settings.EMPTY, mock(RestController.class));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withContent(new BytesArray("{invalid_json}"), XContentType.JSON).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse request body"));
    }

    public void testParseClearScrollRequestWithUnknownParamThrowsException() throws Exception {
        XContentParser invalidContent = createParser(XContentFactory.jsonBuilder()
                .startObject()
                    .array("scroll_id", "value_1", "value_2")
                    .field("unknown", "keyword")
                .endObject());
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();

        Exception e = expectThrows(IllegalArgumentException.class,
                () -> RestClearScrollAction.buildFromContent(invalidContent, clearScrollRequest));
        assertThat(e.getMessage(), startsWith("Unknown parameter [unknown]"));
    }

}
