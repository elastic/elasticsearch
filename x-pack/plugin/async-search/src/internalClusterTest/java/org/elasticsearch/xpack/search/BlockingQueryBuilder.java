/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * A query builder that blocks shard execution based on a {@link QueryLatch}
 * that is shared inside a single jvm (static).
 */
class BlockingQueryBuilder extends AbstractQueryBuilder<BlockingQueryBuilder> {
    public static final String NAME = "block";
    private static QueryLatch queryLatch;

    private final long randomUID;

    /**
     * Creates a new query latch with an expected number of <code>numShardFailures</code>.
     */
    public static synchronized QueryLatch acquireQueryLatch(int numShardFailures) {
        assert queryLatch == null;
        return queryLatch = new QueryLatch(numShardFailures);
    }

    /**
     * Releases the current query latch.
     */
    public static synchronized void releaseQueryLatch() {
        if (queryLatch != null) {
            queryLatch.close();
            queryLatch = null;
        }
    }

    /**
     * Creates a {@link BlockingQueryBuilder} with the provided <code>randomUID</code>.
     */
    BlockingQueryBuilder(long randomUID) {
        super();
        this.randomUID = randomUID;
    }

    BlockingQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.randomUID = in.readLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeLong(randomUID);
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
                try {
                    queryLatch.await(context.getShardId());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
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
    protected boolean doEquals(BlockingQueryBuilder other) {
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

    /**
     *  A synchronization aid that is used by {@link BlockingQueryBuilder} to block shards executions until
     *  the consumer calls {@link QueryLatch#countDownAndReset()}.
     *  The static {@link QueryLatch} is shared in {@link AsyncSearchIntegTestCase#assertBlockingIterator} to provide
     *  a {@link AsyncSearchIntegTestCase.SearchResponseIterator} that unblocks shards executions whenever
     *  {@link AsyncSearchIntegTestCase.SearchResponseIterator#next()} is called.
     */
    static class QueryLatch implements Closeable {
        private final Set<Integer> failedShards = new HashSet<>();
        private volatile CountDownLatch countDownLatch;
        private int numShardFailures;

        private QueryLatch(int numShardFailures) {
            this.countDownLatch = new CountDownLatch(1);
            this.numShardFailures = numShardFailures;
        }

        private void await(int shardId) throws IOException, InterruptedException {
            CountDownLatch last = countDownLatch;
            if (last != null) {
                last.await();
            }
            synchronized (this) {
                // ensure that we fail on replicas too
                if (failedShards.contains(shardId)) {
                    throw new IOException("boom");
                } else if (numShardFailures > 0) {
                    numShardFailures--;
                    failedShards.add(shardId);
                    throw new IOException("boom");
                }
            }
        }

        public synchronized void countDownAndReset() {
            if (countDownLatch != null) {
                CountDownLatch last = countDownLatch;
                countDownLatch = new CountDownLatch(1);
                if (last != null) {
                    assert last.getCount() == 1;
                    last.countDown();
                }
            }
        }

        @Override
        public synchronized void close() {
            if (countDownLatch != null) {
                assert countDownLatch.getCount() == 1;
                countDownLatch.countDown();
            }
            countDownLatch = null;
        }
    }
}
