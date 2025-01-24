/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.io.IOException;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

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
        final RequestOptions expectWarningOptions = expectWarnings(
            "this request accesses system indices: ["
                + TransformInternalIndexConstants.LATEST_INDEX_NAME
                + "], but in a future major version, direct access to system indices will "
                + "be prevented by default"
        ).toBuilder().addHeader("X-elastic-product-origin", "elastic").build();

        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.field(TransformField.ID.getPreferredName(), fakeTransformName);
            }
            builder.endObject();
            final StringEntity entity = new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
            Request req = new Request(
                "PUT",
                TransformInternalIndexConstants.LATEST_INDEX_NAME + "/_doc/" + TransformConfig.documentId(fakeTransformName)
            );
            req.setOptions(expectWarningOptions);
            req.setEntity(entity);
            client().performRequest(req);
        }

        // refresh the index
        final Request refreshRequest = new Request("POST", TransformInternalIndexConstants.LATEST_INDEX_NAME + "/_refresh");
        refreshRequest.setOptions(expectWarningOptions);
        assertOK(client().performRequest(refreshRequest));

        Request deleteRequest = new Request("DELETE", getTransformEndpoint() + fakeTransformName);
        Response deleteResponse = client().performRequest(deleteRequest);
        assertOK(deleteResponse);
        assertTrue((boolean) XContentMapValues.extractValue("acknowledged", entityAsMap(deleteResponse)));

        // delete again, should fail
        expectThrows(ResponseException.class, () -> client().performRequest(deleteRequest));
    }
}
