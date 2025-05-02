/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules.retriever;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankDoc;

import java.io.IOException;
import java.util.Objects;

public class PinnedRankDoc extends RankDoc {
    public static final String NAME = "pinned_rank_doc";

    private final boolean isPinned;

    public PinnedRankDoc(int docId, float score, int shardIndex, boolean isPinned) {
        super(docId, score, shardIndex);
        this.isPinned = isPinned;
    }

    public PinnedRankDoc(StreamInput in) throws IOException {
        super(in);
        this.isPinned = in.readBoolean();
    }

    public boolean isPinned() {
        return isPinned;
    }

    @Override
    public Explanation explain(Explanation[] sources, String[] queryNames) {
        if (isPinned) {
            return Explanation.match(score, "Pinned document, original explanation:", sources);
        } else {
            return super.explain(sources, queryNames);
        }
    }

    @Override
    public String toString() {
        return super.toString() + ", isPinned=" + isPinned;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(isPinned);
    }

    @Override
    protected boolean doEquals(RankDoc rd) {
        if (rd instanceof PinnedRankDoc other) {
            return this.isPinned == other.isPinned;
        } else {
            return false;
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.doHashCode(), isPinned);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.PINNED_RETRIEVER_8_19;
    }
}
