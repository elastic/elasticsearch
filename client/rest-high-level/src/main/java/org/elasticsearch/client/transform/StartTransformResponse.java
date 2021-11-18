/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class StartTransformResponse extends AcknowledgedTasksResponse {

    private static final String ACKNOWLEDGED = "acknowledged";

    private static final ConstructingObjectParser<StartTransformResponse, Void> PARSER = AcknowledgedTasksResponse.generateParser(
        "start_transform_response",
        StartTransformResponse::new,
        ACKNOWLEDGED
    );

    public static StartTransformResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public StartTransformResponse(
        boolean acknowledged,
        @Nullable List<TaskOperationFailure> taskFailures,
        @Nullable List<? extends ElasticsearchException> nodeFailures
    ) {
        super(acknowledged, taskFailures, nodeFailures);
    }

}
