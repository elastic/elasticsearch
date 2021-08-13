/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

public class GetRollupIndexCapsResponseTests extends RollupCapsResponseTestCase<GetRollupIndexCapsResponse> {

    @Override
    protected GetRollupIndexCapsResponse createTestInstance() {
        return new GetRollupIndexCapsResponse(indices);
    }

    @Override
    protected void toXContent(GetRollupIndexCapsResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        for (Map.Entry<String, RollableIndexCaps> entry : response.getJobs().entrySet()) {
            entry.getValue().toXContent(builder, null);
        }
        builder.endObject();
    }

    @Override
    protected Predicate<String> randomFieldsExcludeFilter() {
        return (field) ->
        {
            // base cannot have extra things in it
            return "".equals(field)
                // the field list expects to be a nested object of a certain type
                || field.contains("fields");
        };
    }

    @Override
    protected GetRollupIndexCapsResponse fromXContent(XContentParser parser) throws IOException {
        return GetRollupIndexCapsResponse.fromXContent(parser);
    }
}
