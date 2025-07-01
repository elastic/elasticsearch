/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;

/**
 * A {@link QueryBuilder} that wraps another {@linkplain QueryBuilder}
 * so it read with a {@link PlanStreamInput}.
 */
public class PlanStreamWrapperQueryBuilder implements QueryBuilder {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        "planwrapper",
        PlanStreamWrapperQueryBuilder::new
    );

    private final Configuration configuration;
    private final QueryBuilder next;

    public PlanStreamWrapperQueryBuilder(Configuration configuration, QueryBuilder next) {
        LogManager.getLogger(PlanStreamWrapperQueryBuilder.class).error("ASFAFDSAFDSF wrap {}", next);
        this.configuration = configuration;
        this.next = next;
    }

    public PlanStreamWrapperQueryBuilder(StreamInput in) throws IOException {
        configuration = Configuration.readWithoutTables(in);
        LogManager.getLogger(PlanStreamWrapperQueryBuilder.class).error("ASFAFDSAFDSF streamread {}", configuration);
        PlanStreamInput planStreamInput = new PlanStreamInput(in, in.namedWriteableRegistry(), configuration);
        next = planStreamInput.readNamedWriteable(QueryBuilder.class);
        LogManager.getLogger(PlanStreamWrapperQueryBuilder.class).error("ASFAFDSAFDSF stream {}", next);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        configuration.withoutTables().writeTo(out);
        new PlanStreamOutput(out, configuration).writeNamedWriteable(next);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ESQL_FIXED_INDEX_LIKE;
    }

    @Override
    public Query toQuery(SearchExecutionContext context) throws IOException {
        return next.toQuery(context);
    }

    @Override
    public QueryBuilder queryName(String queryName) {
        next.queryName(queryName);
        return this;
    }

    @Override
    public String queryName() {
        return next.queryName();
    }

    @Override
    public float boost() {
        return next.boost();
    }

    @Override
    public QueryBuilder boost(float boost) {
        next.boost(boost);
        return this;
    }

    @Override
    public String getName() {
        return getWriteableName();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return next.toXContent(builder, params);
    }

    public QueryBuilder next() {
        return next;
    }
}
