/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.rollup.GetRollupCapsResponse;
import org.elasticsearch.protocol.xpack.rollup.RollableIndexCaps;
import org.elasticsearch.protocol.xpack.rollup.RollupJobCaps;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetRollupCapsResponseTests extends AbstractStreamableXContentTestCase<GetRollupCapsResponse> {

    private Map<String, RollableIndexCaps> indices;

    @Before
    private void setupIndices() {
        int numIndices = randomIntBetween(1,5);
        indices = new HashMap<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            String indexName = "index_" + randomAlphaOfLength(10);
            int numJobs = randomIntBetween(1,5);
            List<RollupJobCaps> jobs = new ArrayList<>(numJobs);
            for (int j = 0; j < numJobs; j++) {
                jobs.add(new RollupJobCaps(ConfigTestHelpers.randomRollupJobConfig(random())));
            }
            RollableIndexCaps cap = new RollableIndexCaps(indexName, jobs);
            indices.put(indexName, cap);
        }
    }

    @Override
    protected GetRollupCapsResponse createTestInstance() {
        return new GetRollupCapsResponse(indices);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected GetRollupCapsResponse createBlankInstance() {
        return new GetRollupCapsResponse();
    }

    @Override
    protected GetRollupCapsResponse doParseInstance(final XContentParser parser) throws IOException {
        return GetRollupCapsResponse.fromXContent(parser);
    }


}
