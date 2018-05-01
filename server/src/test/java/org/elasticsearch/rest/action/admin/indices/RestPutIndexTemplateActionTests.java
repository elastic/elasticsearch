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

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.InvalidTemplateNameException;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestPutIndexTemplateActionTests extends ESTestCase {

    public void testInvalidTemplateNameFails() {
        Settings settings = settings(Version.CURRENT).build();
        RestPutIndexTemplateAction action = new RestPutIndexTemplateAction(settings, mock(RestController.class));

        final RestRequest request = new RestRequest(NamedXContentRegistry.EMPTY,
            Collections.singletonMap("name", "*"),
            "_template/*",
            Collections.emptyMap()) {

            @Override
            public RestRequest.Method method() {
                return RestRequest.Method.GET;
            }

            @Override
            public String uri() {
                return null;
            }

            @Override
            public boolean hasContent() {
                return true;
            }

            @Override
            public BytesReference content() {
                return null;
            }
        };

        InvalidTemplateNameException e = expectThrows(InvalidTemplateNameException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class)));
        assertThat(e.getMessage(),
            equalTo("Invalid template name [*], must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?]"));
    }
}
