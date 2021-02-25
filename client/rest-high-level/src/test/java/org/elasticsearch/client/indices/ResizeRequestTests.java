/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.client.AbstractRequestTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;

public class ResizeRequestTests extends AbstractRequestTestCase<ResizeRequest,
    org.elasticsearch.action.admin.indices.shrink.ResizeRequest> {

    @Override
    protected ResizeRequest createClientTestInstance() {
        return new ResizeRequest("target", "source")
            .setAliases(Arrays.asList(new Alias("target1"), new Alias("target2")))
            .setSettings(Settings.builder().put("index.foo", "bar").build());
    }

    @Override
    protected org.elasticsearch.action.admin.indices.shrink.ResizeRequest doParseToServerInstance(XContentParser parser)
        throws IOException {
        org.elasticsearch.action.admin.indices.shrink.ResizeRequest req
            = new org.elasticsearch.action.admin.indices.shrink.ResizeRequest("target", "source");
        req.fromXContent(parser);
        return req;
    }

    @Override
    protected void assertInstances(org.elasticsearch.action.admin.indices.shrink.ResizeRequest serverInstance,
                                   ResizeRequest clientTestInstance) {
        assertEquals(serverInstance.getSourceIndex(), clientTestInstance.getSourceIndex());
        assertEquals(serverInstance.getTargetIndexRequest().index(), clientTestInstance.getTargetIndex());
        assertEquals(serverInstance.getTargetIndexRequest().settings(), clientTestInstance.getSettings());
        assertEquals(serverInstance.getTargetIndexRequest().aliases(), clientTestInstance.getAliases());
    }
}
