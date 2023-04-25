/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class TestRankShardResult implements RankShardResult {

    public final TestRankDoc[] testRankDocs;

    public TestRankShardResult(TestRankDoc[] testRankDocs) {
        this.testRankDocs = testRankDocs;
    }

    public TestRankShardResult(StreamInput in) throws IOException {
        testRankDocs = in.readArray(TestRankDoc::new, TestRankDoc[]::new);
    }

    @Override
    public String getWriteableName() {
        return TestRankBuilder.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_8_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(testRankDocs);
    }
}
