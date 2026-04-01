/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.DataInput;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ShardFailingQueryBuilder extends AbstractQueryBuilder<ShardFailingQueryBuilder> {
    public static final String NAME = "shard_failing_query";

    public static ShardFailingQueryBuilder fromXContent(XContentParser parser) {
        return new ShardFailingQueryBuilder();
    }

    public ShardFailingQueryBuilder() {}

    public ShardFailingQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.current();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {

    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.endObject();
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if (context.getShardId() % 2 == 0) {
            throw new CorruptIndexException("simulated failure", (DataInput) null);
        } else {
            return new MatchAllDocsQuery();
        }
    }

    @Override
    protected boolean doEquals(ShardFailingQueryBuilder other) {
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }
}
