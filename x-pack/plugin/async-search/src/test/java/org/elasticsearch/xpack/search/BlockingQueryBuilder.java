/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;

/**
 * A query builder that blocks shard execution based on the provided {@link ShardIdLatch}.
 */
class BlockingQueryBuilder extends AbstractQueryBuilder<BlockingQueryBuilder> {
    public static final String NAME = "block";
    private final Map<ShardId, ShardIdLatch> shardsLatch;

    BlockingQueryBuilder(Map<ShardId, ShardIdLatch> shardsLatch) {
        super();
        this.shardsLatch = shardsLatch;
    }

    BlockingQueryBuilder(StreamInput in, Map<ShardId, ShardIdLatch> shardsLatch) throws IOException {
        super(in);
        this.shardsLatch = shardsLatch;
    }

    BlockingQueryBuilder() {
        this.shardsLatch = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) {}

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.endObject();
    }

    private static final ObjectParser<BlockingQueryBuilder, Void> PARSER = new ObjectParser<>(NAME, BlockingQueryBuilder::new);

    public static BlockingQueryBuilder fromXContent(XContentParser parser, Map<ShardId, ShardIdLatch> shardsLatch) {
        try {
            PARSER.apply(parser, null);
            return new BlockingQueryBuilder(shardsLatch);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(parser.getTokenLocation(), e.getMessage(), e);
        }
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        final Query delegate = Queries.newMatchAllQuery();
        return new Query() {
            @Override
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
                if (shardsLatch != null) {
                    try {
                        final ShardIdLatch latch = shardsLatch.get(new ShardId(context.index(), context.getShardId()));
                        latch.await();
                        if (latch.shouldFail()) {
                            throw new IOException("boum");
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
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
}
