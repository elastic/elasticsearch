/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.transport;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.action.compute.data.Page;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;

import java.util.function.Consumer;

public class ComputeRequest extends SingleShardRequest<ComputeRequest> {

    public QueryBuilder query; // FROM clause (+ additional pushed down filters)
    public Aggs aggs;
    public long nowInMillis;

    private final Consumer<Page> pageConsumer; // quick hack to stream responses back

    public ComputeRequest(StreamInput in) {
        throw new UnsupportedOperationException();
    }

    public ComputeRequest(String index, QueryBuilder query, Aggs aggs, Consumer<Page> pageConsumer) {
        super(index);
        this.query = query;
        this.aggs = aggs;
        this.pageConsumer = pageConsumer;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public QueryBuilder query() {
        return query;
    }

    public void query(QueryBuilder query) {
        this.query = query;
    }

    public Consumer<Page> getPageConsumer() {
        return pageConsumer;
    }
}
