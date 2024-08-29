/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * {@code RankDoc} is the base class for all ranked results.
 * Subclasses should extend this with additional information required for their global ranking method.
 */
public class RankDoc extends ScoreDoc implements NamedWriteable, ToXContentFragment {

    public static final String NAME = "rank_doc";

    public static final int NO_RANK = -1;

    /**
     * If this document has been ranked, this is its final rrf ranking from all the result sets.
     */
    public int rank = NO_RANK;

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public record RankKey(int doc, int shardIndex) {}

    public RankDoc(int doc, float score, int shardIndex) {
        super(doc, score, shardIndex);
    }

    public RankDoc(StreamInput in) throws IOException {
        super(in.readVInt(), in.readFloat(), in.readVInt());
        rank = in.readVInt();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(doc);
        out.writeFloat(score);
        out.writeVInt(shardIndex);
        out.writeVInt(rank);
        doWriteTo(out);
    }

    protected void doWriteTo(StreamOutput out) throws IOException {};

    /**
     * Explain the ranking of this document.
     */
    public Explanation explain() {
        return Explanation.match(rank, "doc [" + doc + "] with an original score of [" + score + "] is at rank [" + rank + "].");
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("_rank", rank);
        builder.field("_doc", doc);
        builder.field("_shard", shardIndex);
        builder.field("_score", score);
        doToXContent(builder, params);
        return builder;
    }

    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {}

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RankDoc rd = (RankDoc) o;
        return doc == rd.doc && score == rd.score && shardIndex == rd.shardIndex && rank == rd.rank && doEquals(rd);
    }

    protected boolean doEquals(RankDoc rd) {
        return true;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(doc, score, shardIndex, doHashCode());
    }

    protected int doHashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "RankDoc{" + "_rank=" + rank + ", _doc=" + doc + ", _shard=" + shardIndex + ", _score=" + score + '}';
    }
}
