/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Generic wrapper class for a page of query results and the total number of
 * query hits.<br>
 * {@linkplain #hitCount()} is the total number of results but that value may
 * not be equal to the actual length of the {@linkplain #hits()} list if from
 * &amp; take or some cursor was used in the database query.
 */
public final class QueryPage<T extends ToXContent & Writeable> extends ToXContentToBytes implements Writeable {

    public static final ParseField HITS = new ParseField("hits");
    public static final ParseField HIT_COUNT = new ParseField("hitCount");

    private final List<T> hits;
    private final long hitCount;

    public QueryPage(List<T> hits, long hitCount) {
        this.hits = hits;
        this.hitCount = hitCount;
    }

    public QueryPage(StreamInput in, Reader<T> hitReader) throws IOException {
        hits = in.readList(hitReader);
        hitCount = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(hits);
        out.writeLong(hitCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(HITS.getPreferredName(), hits);
        builder.field(HIT_COUNT.getPreferredName(), hitCount);
        return builder;
    }

    public List<T> hits() {
        return hits;
    }

    public long hitCount() {
        return hitCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(hits, hitCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        @SuppressWarnings("unchecked")
        QueryPage<T> other = (QueryPage<T>) obj;
        return Objects.equals(hits, other.hits) &&
                Objects.equals(hitCount, other.hitCount);
    }
}
