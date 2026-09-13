/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a single outlier candidate detected on a shard.
 * Carries the document ID, its projected vector, and the kNN-distance outlier score.
 */
public class OutlierCandidate implements Writeable, ToXContentObject, Comparable<OutlierCandidate> {

    private final String docId;
    private final float[] projectedVector;
    private final double score;
    private final int shardIndex;

    public OutlierCandidate(String docId, float[] projectedVector, double score, int shardIndex) {
        this.docId = Objects.requireNonNull(docId);
        this.projectedVector = Objects.requireNonNull(projectedVector);
        this.score = score;
        this.shardIndex = shardIndex;
    }

    public OutlierCandidate(StreamInput in) throws IOException {
        this.docId = in.readString();
        this.projectedVector = in.readFloatArray();
        this.score = in.readDouble();
        this.shardIndex = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(docId);
        out.writeFloatArray(projectedVector);
        out.writeDouble(score);
        out.writeVInt(shardIndex);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("doc_id", docId);
        builder.field("score", score);
        builder.field("shard_index", shardIndex);
        builder.endObject();
        return builder;
    }

    public String getDocId() {
        return docId;
    }

    public float[] getProjectedVector() {
        return projectedVector;
    }

    public double getScore() {
        return score;
    }

    public int getShardIndex() {
        return shardIndex;
    }

    /**
     * Higher scores are "more outlier" — natural ordering is descending by score.
     */
    @Override
    public int compareTo(OutlierCandidate other) {
        return Double.compare(other.score, this.score);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutlierCandidate that = (OutlierCandidate) o;
        return Double.compare(that.score, score) == 0 && shardIndex == that.shardIndex && docId.equals(that.docId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(docId, score, shardIndex);
    }
}
