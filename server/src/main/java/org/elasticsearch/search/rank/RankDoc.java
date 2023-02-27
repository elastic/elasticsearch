/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class RankDoc extends ScoreDoc implements ToXContent, NamedWriteable {

    public RankDoc(int doc, float score, int shardIndex) {
        super(doc, score, shardIndex);
    }

    protected RankDoc(StreamInput in) throws IOException {
        super(in.readVInt(), in.readFloat(), in.readVInt());
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return doToXContent(builder, params);
    }

    public abstract XContentBuilder doToXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(doc);
        out.writeFloat(score);
        out.writeVInt(shardIndex);
        doWriteTo(out);
    }

    public abstract void doWriteTo(StreamOutput out) throws IOException;
}
