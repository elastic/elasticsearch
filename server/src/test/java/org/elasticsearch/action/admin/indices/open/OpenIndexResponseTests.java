/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.open;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.rest.TestResponseParsers;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

public class OpenIndexResponseTests extends AbstractXContentSerializingTestCase<OpenIndexResponse> {

    private static final ConstructingObjectParser<OpenIndexResponse, Void> PARSER = new ConstructingObjectParser<>(
        "open_index",
        true,
        args -> new OpenIndexResponse((boolean) args[0], (boolean) args[1])
    );

    static {
        TestResponseParsers.declareAcknowledgedAndShardsAcknowledgedFields(PARSER);
    }

    @Override
    protected OpenIndexResponse doParseInstance(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    protected OpenIndexResponse createTestInstance() {
        boolean acknowledged = randomBoolean();
        boolean shardsAcknowledged = acknowledged && randomBoolean();
        return new OpenIndexResponse(acknowledged, shardsAcknowledged);
    }

    @Override
    protected Writeable.Reader<OpenIndexResponse> instanceReader() {
        return OpenIndexResponse::new;
    }

    @Override
    protected OpenIndexResponse mutateInstance(OpenIndexResponse response) {
        if (randomBoolean()) {
            boolean acknowledged = response.isAcknowledged() == false;
            boolean shardsAcknowledged = acknowledged && response.isShardsAcknowledged();
            return new OpenIndexResponse(acknowledged, shardsAcknowledged);
        } else {
            boolean shardsAcknowledged = response.isShardsAcknowledged() == false;
            boolean acknowledged = shardsAcknowledged || response.isAcknowledged();
            return new OpenIndexResponse(acknowledged, shardsAcknowledged);
        }
    }
}
