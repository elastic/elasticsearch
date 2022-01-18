/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.shrink;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

public class ResizeResponseTests extends AbstractSerializingTestCase<ResizeResponse> {

    public void testToXContent() {
        ResizeResponse response = new ResizeResponse(true, false, "index_name");
        String output = Strings.toString(response);
        assertEquals("""
            {"acknowledged":true,"shards_acknowledged":false,"index":"index_name"}""", output);
    }

    @Override
    protected ResizeResponse doParseInstance(XContentParser parser) {
        return ResizeResponse.fromXContent(parser);
    }

    @Override
    protected ResizeResponse createTestInstance() {
        boolean acknowledged = randomBoolean();
        boolean shardsAcknowledged = acknowledged && randomBoolean();
        String index = randomAlphaOfLength(5);
        return new ResizeResponse(acknowledged, shardsAcknowledged, index);
    }

    @Override
    protected Writeable.Reader<ResizeResponse> instanceReader() {
        return ResizeResponse::new;
    }

    @Override
    protected ResizeResponse mutateInstance(ResizeResponse response) {
        if (randomBoolean()) {
            if (randomBoolean()) {
                boolean acknowledged = response.isAcknowledged() == false;
                boolean shardsAcknowledged = acknowledged && response.isShardsAcknowledged();
                return new ResizeResponse(acknowledged, shardsAcknowledged, response.index());
            } else {
                boolean shardsAcknowledged = response.isShardsAcknowledged() == false;
                boolean acknowledged = shardsAcknowledged || response.isAcknowledged();
                return new ResizeResponse(acknowledged, shardsAcknowledged, response.index());
            }
        } else {
            return new ResizeResponse(
                response.isAcknowledged(),
                response.isShardsAcknowledged(),
                response.index() + randomAlphaOfLengthBetween(2, 5)
            );
        }
    }
}
