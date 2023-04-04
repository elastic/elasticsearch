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

public class TestRankDoc extends RankDoc {

    public TestRankDoc(int doc, float score, int shardIndex) {
        super(doc, score, shardIndex);
    }

    public TestRankDoc(StreamInput in) throws IOException {
        super(in);
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
    public void doWriteTo(StreamOutput out) throws IOException {
        // do nothing
    }

    @Override
    public boolean doEquals(Object o) {
        return true;
    }

    @Override
    public int doHashCode() {
        return 0;
    }
}
