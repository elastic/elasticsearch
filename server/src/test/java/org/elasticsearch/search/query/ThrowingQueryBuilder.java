/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.query;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A query builder that can throw an exception on a targeted shard or a targeted index.
 * It can also put a sleep into the query (createWeight) method of another specified index
 * (or all indexes using "*" wildcard) if that index or shard is not throwing an Exception.
 */
public class ThrowingQueryBuilder extends AbstractQueryBuilder<ThrowingQueryBuilder> {
    public static final String NAME = "throw";

    private final long randomUID;
    private final RuntimeException failure;
    private final int shardId;
    private final String exceptionIndex; // index to throw the Exception for

    private final String sleepIndex;
    private final long sleepTime;

    /**
     * Creates a {@link ThrowingQueryBuilder} with the provided <code>randomUID</code>.
     *
     * @param randomUID used solely for identification
     * @param failure what exception to throw
     * @param shardId what shardId to throw the exception. If shardId is less than 0, it will throw for all shards.
     */
    public ThrowingQueryBuilder(long randomUID, RuntimeException failure, int shardId) {
        this(randomUID, failure, shardId, null, 0, null);
    }

    /**
     * Creates a {@link ThrowingQueryBuilder} with the provided <code>randomUID</code>.
     *
     * @param randomUID used solely for identification
     * @param failure what exception to throw
     * @param index what index to throw the exception against (all shards of that index)
     */
    public ThrowingQueryBuilder(long randomUID, RuntimeException failure, String index) {
        this(randomUID, failure, Integer.MAX_VALUE, index, 0, null);
    }

    public ThrowingQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.randomUID = in.readLong();
        this.failure = in.readException();
        this.shardId = in.readVInt();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_500_040)) {
            this.exceptionIndex = in.readOptionalString();
        } else {
            this.exceptionIndex = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.THROWING_QUERY_BUILDER_SLEEPS_ADDED)) {
            this.sleepTime = in.readVLong();
            this.sleepIndex = in.readOptionalString();
        } else {
            this.sleepTime = 0;
            this.sleepIndex = null;
        }
    }

    private ThrowingQueryBuilder(
        long randomUID,
        RuntimeException exception,
        int exceptionShardId,
        String exceptionIndex,
        long sleepTime,
        String sleepIndex
    ) {
        super();
        this.randomUID = randomUID;
        this.failure = exception;
        this.shardId = exceptionShardId;
        this.exceptionIndex = exceptionIndex;
        this.sleepTime = sleepTime;
        this.sleepIndex = sleepIndex;
    }

    /**
     * Use the Builder when you want to use multiple features of ThrowingQueryBuilder such as
     * throwing an Exception on one index and sleeping on others
     */
    public static class Builder {
        private final long randomUID;
        private RuntimeException failure;
        private int shardId = Integer.MAX_VALUE;  // -1 means "all shards", so this defaults to a non-existent shard
        private String exceptionIndex;
        private String sleepIndex;
        private long sleepTime;

        public Builder(long randomUID) {
            this.randomUID = randomUID;
        }

        public ThrowingQueryBuilder build() {
            return new ThrowingQueryBuilder(randomUID, failure, shardId, exceptionIndex, sleepTime, sleepIndex);
        }

        public Builder setExceptionForShard(int shardId, RuntimeException exception) {
            this.shardId = shardId;
            this.failure = exception;
            return this;
        }

        public Builder setExceptionForIndex(String index, RuntimeException exception) {
            this.exceptionIndex = index;
            this.failure = exception;
            return this;
        }

        public Builder setSleepForIndex(String index, long sleepTime) {
            this.sleepIndex = index;
            this.sleepTime = sleepTime;
            return this;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeLong(randomUID);
        out.writeException(failure);
        out.writeVInt(shardId);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_500_040)) {
            out.writeOptionalString(exceptionIndex);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.THROWING_QUERY_BUILDER_SLEEPS_ADDED)) {
            out.writeVLong(sleepTime);
            out.writeOptionalString(sleepIndex);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        final Query delegate = Queries.newMatchAllQuery();
        return new Query() {
            @Override
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
                if (context.getShardId() == shardId || shardId < 0 || context.index().getName().equals(exceptionIndex)) {
                    throw failure;
                }
                if (context.index().getName().equals(sleepIndex) || "*".equals(sleepIndex)) {
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                return delegate.createWeight(searcher, scoreMode, boost);
            }

            @Override
            public String toString(String field) {
                return delegate.toString(field);
            }

            @Override
            public boolean equals(Object obj) {
                return false;
            }

            @Override
            public int hashCode() {
                return 0;
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.visitLeaf(this);
            }
        };
    }

    @Override
    protected boolean doEquals(ThrowingQueryBuilder other) {
        return false;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
