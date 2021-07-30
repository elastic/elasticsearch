/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.watcher.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.watcher.DeleteWatchResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DeleteWatchResponseTests extends AbstractResponseTestCase<
    org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse, DeleteWatchResponse> {

    @Override
    protected org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse createServerTestInstance(XContentType xContentType) {
        String id = randomAlphaOfLength(10);
        long version = randomLongBetween(1, 10);
        boolean found = randomBoolean();
        return new org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse(id, version, found);
    }

    @Override
    protected DeleteWatchResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return DeleteWatchResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse serverTestInstance,
                                   DeleteWatchResponse clientInstance) {
        assertThat(clientInstance.getId(), equalTo(serverTestInstance.getId()));
        assertThat(clientInstance.getVersion(), equalTo(serverTestInstance.getVersion()));
        assertThat(clientInstance.isFound(), equalTo(serverTestInstance.isFound()));
    }
}
