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

package org.elasticsearch.rest.action;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class RestBuilderListenerTests extends ESTestCase {

    public void testThatXContentBuilderIsClosed() throws Exception {
        AtomicReference<XContentBuilder> builderAtomicReference = new AtomicReference<>();
        RestBuilderListener<TransportResponse.Empty> builderListener =
            new RestBuilderListener<Empty>(new FakeRestChannel(new FakeRestRequest(), randomBoolean(), 1)) {
            @Override
            public RestResponse buildResponse(Empty empty, XContentBuilder builder) throws Exception {
                builderAtomicReference.set(builder);
                // write some bad data that will cause an exception to be thrown on the close of the builder
                builder.startObject().field("foo", "bar");
                return new BytesRestResponse(RestStatus.OK, BytesRestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);
            }
        };

        // TODO unfortunately this isn't very easy to test but we can at least verify that close is called
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> builderListener.buildResponse(Empty.INSTANCE));
        assertThat(e.getMessage(), equalTo("Failed to close the XContentBuilder"));
        assertNotNull(builderAtomicReference.get());
    }
}
