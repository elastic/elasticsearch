/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.open;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * A response for a open index action.
 */
public class OpenIndexResponse extends ShardsAcknowledgedResponse {

    private static final ConstructingObjectParser<OpenIndexResponse, Void> PARSER = new ConstructingObjectParser<>(
        "open_index",
        true,
        args -> new OpenIndexResponse((boolean) args[0], (boolean) args[1])
    );

    static {
        declareAcknowledgedAndShardsAcknowledgedFields(PARSER);
    }

    public OpenIndexResponse(StreamInput in) throws IOException {
        super(in, in.getVersion().onOrAfter(Version.V_6_1_0), true);
    }

    public OpenIndexResponse(boolean acknowledged, boolean shardsAcknowledged) {
        super(acknowledged, shardsAcknowledged);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
            writeShardsAcknowledged(out);
        }
    }

    public static OpenIndexResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
