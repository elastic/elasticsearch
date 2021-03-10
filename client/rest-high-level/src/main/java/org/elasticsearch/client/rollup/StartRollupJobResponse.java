/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.rollup;

import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class StartRollupJobResponse extends AcknowledgedResponse {

    private static final String PARSE_FIELD_NAME = "started";

    private static final ConstructingObjectParser<StartRollupJobResponse, Void> PARSER = AcknowledgedResponse
            .generateParser("start_rollup_job_response", StartRollupJobResponse::new, PARSE_FIELD_NAME);

    public StartRollupJobResponse(boolean acknowledged) {
        super(acknowledged);
    }

    public static StartRollupJobResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    protected String getFieldName() {
        return PARSE_FIELD_NAME;
    }
}
