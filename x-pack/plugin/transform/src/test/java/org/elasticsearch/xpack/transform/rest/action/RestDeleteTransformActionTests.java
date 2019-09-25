/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestDeleteTransformActionTests extends ESTestCase {

    public void testBodyRejection() throws Exception {
        final RestDeleteTransformAction handler = new RestDeleteTransformAction(
            mock(RestController.class));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.field("id", "my_id");
            }
            builder.endObject();
            final FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
                    .withContent(new BytesArray(builder.toString()), XContentType.JSON)
                    .build();
            IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> handler.prepareRequest(request, mock(NodeClient.class)));
            assertThat(e.getMessage(), equalTo("delete transform requests can not have a request body"));
        }
    }

}
