/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.transform.UpgradeTransformsResponse;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.action.UpgradeTransformsAction.Response;

import java.io.IOException;

public class UpgradeTransformsResponseTests extends AbstractResponseTestCase<
    Response,
    org.elasticsearch.client.transform.UpgradeTransformsResponse> {

    public static Response randomUpgradeResponse() {
        return new Response(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected Response createServerTestInstance(XContentType xContentType) {
        return randomUpgradeResponse();
    }

    @Override
    protected UpgradeTransformsResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.transform.UpgradeTransformsResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(Response serverTestInstance, UpgradeTransformsResponse clientInstance) {
        assertEquals(serverTestInstance.getNeedsUpdate(), clientInstance.getNeedsUpdate());
        assertEquals(serverTestInstance.getNoAction(), clientInstance.getNoAction());
        assertEquals(serverTestInstance.getUpdated(), clientInstance.getUpdated());
    }

}
