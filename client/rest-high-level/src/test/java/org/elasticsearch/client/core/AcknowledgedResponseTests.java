/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.core;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class AcknowledgedResponseTests extends AbstractResponseTestCase<org.elasticsearch.action.support.master.AcknowledgedResponse,
    AcknowledgedResponse> {

    @Override
    protected org.elasticsearch.action.support.master.AcknowledgedResponse createServerTestInstance(XContentType xContentType) {
        return org.elasticsearch.action.support.master.AcknowledgedResponse.of(randomBoolean());
    }

    @Override
    protected AcknowledgedResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return AcknowledgedResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.action.support.master.AcknowledgedResponse serverTestInstance,
                                   AcknowledgedResponse clientInstance) {
        assertThat(clientInstance.isAcknowledged(), is(serverTestInstance.isAcknowledged()));
    }

    // Still needed for StopRollupJobResponseTests and StartRollupJobResponseTests test classes
    // This method can't be moved to these classes because getFieldName() method isn't accessible from these test classes.
    public static void toXContent(AcknowledgedResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        {
            builder.field(response.getFieldName(), response.isAcknowledged());
        }
        builder.endObject();
    }

}
