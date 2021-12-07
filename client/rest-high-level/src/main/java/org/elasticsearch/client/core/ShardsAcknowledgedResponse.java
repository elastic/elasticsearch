/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.core;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ShardsAcknowledgedResponse extends AcknowledgedResponse {

    protected static final String SHARDS_PARSE_FIELD_NAME = "shards_acknowledged";

    private static ConstructingObjectParser<ShardsAcknowledgedResponse, Void> buildParser() {

        ConstructingObjectParser<ShardsAcknowledgedResponse, Void> p = new ConstructingObjectParser<>(
            "freeze",
            true,
            args -> new ShardsAcknowledgedResponse((boolean) args[0], (boolean) args[1])
        );
        p.declareBoolean(constructorArg(), new ParseField(AcknowledgedResponse.PARSE_FIELD_NAME));
        p.declareBoolean(constructorArg(), new ParseField(SHARDS_PARSE_FIELD_NAME));
        return p;
    }

    private static final ConstructingObjectParser<ShardsAcknowledgedResponse, Void> PARSER = buildParser();

    private final boolean shardsAcknowledged;

    public ShardsAcknowledgedResponse(boolean acknowledged, boolean shardsAcknowledged) {
        super(acknowledged);
        this.shardsAcknowledged = shardsAcknowledged;
    }

    public boolean isShardsAcknowledged() {
        return shardsAcknowledged;
    }

    public static ShardsAcknowledgedResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
