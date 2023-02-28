/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rrf;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class RRFRankDoc extends RankDoc {

    public int rank;
    public final int[] positions;
    public final float[] scores;

    public RRFRankDoc(int doc, int shardIndex, int size) {
        super(doc, 0f, shardIndex);
        positions = new int[size];
        scores = new float[size];
    }

    public RRFRankDoc(StreamInput in) throws IOException {
        super(in);
        rank = in.readVInt();
        positions = in.readIntArray();
        scores = in.readFloatArray();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(rank);
        out.writeIntArray(positions);
        out.writeFloatArray(scores);
    }

    @Override
    public XContentBuilder doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array("positions", positions);
        builder.array("scores", scores);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return RRFRankContextBuilder.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_8_0;
    }
}
