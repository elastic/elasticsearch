/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TransformConfigurationIndexIT extends TransformRestTestCase {

    /**
     * Tests the corner case that for some reason a transform configuration still exists in the index but
     * the persistent task disappeared
     *
     * test note: {@link TransformRestTestCase} checks for an empty index as part of the test case cleanup,
     * so we do not need to check that the document has been deleted in this place
     */
    public void testDeleteConfigurationLeftOver() throws IOException {
        String fakeTransformName = randomAlphaOfLengthBetween(5, 20);

        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.field(TransformField.ID.getPreferredName(), fakeTransformName);
            }
            builder.endObject();
            final StringEntity entity = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
            Request req = new Request("PUT",
                    TransformInternalIndexConstants.LATEST_INDEX_NAME + "/_doc/" + TransformConfig.documentId(fakeTransformName));
            req.setEntity(entity);
            client().performRequest(req);
        }

        // refresh the index
        assertOK(client().performRequest(new Request("POST", TransformInternalIndexConstants.LATEST_INDEX_NAME + "/_refresh")));

        Request deleteRequest = new Request("DELETE", getTransformEndpoint() + fakeTransformName);
        Response deleteResponse = client().performRequest(deleteRequest);
        assertOK(deleteResponse);
        assertTrue((boolean)XContentMapValues.extractValue("acknowledged", entityAsMap(deleteResponse)));

        // delete again, should fail
        expectThrows(ResponseException.class,() -> client().performRequest(deleteRequest));
    }
}
