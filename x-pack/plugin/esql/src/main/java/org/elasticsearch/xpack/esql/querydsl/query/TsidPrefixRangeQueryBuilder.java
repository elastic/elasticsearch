/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.compute.lucene.query.TsidPrefixRangeQuery;
import org.elasticsearch.index.query.LeafQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A query builder of {@link TsidPrefixRangeQuery}.
 */
public class TsidPrefixRangeQueryBuilder extends LeafQueryBuilder<TsidPrefixRangeQueryBuilder> {
    public static final TransportVersion TSID_PREFIX_RANGE_QUERY = TransportVersion.fromName("tsid_prefix_range_query");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        QueryBuilder.class,
        "esql_tsid_prefix_range",
        TsidPrefixRangeQueryBuilder::new
    );

    private final TsidPrefixRange range;

    public TsidPrefixRangeQueryBuilder(TsidPrefixRange range) {
        this.range = range;
    }

    TsidPrefixRangeQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.range = new TsidPrefixRange(in.readInt(), in.readInt(), in.readInt());
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeInt(range.prefixBits());
        out.writeInt(range.lower());
        out.writeInt(range.upper());
    }

    public TsidPrefixRange range() {
        return range;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) {
        return new TsidPrefixRangeQuery(range);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ENTRY.name);
        builder.field("prefix_bits", range.prefixBits());
        builder.field("lower", range.lower());
        builder.field("upper", range.upper());
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TSID_PREFIX_RANGE_QUERY;
    }

    @Override
    protected boolean doEquals(TsidPrefixRangeQueryBuilder other) {
        return range.equals(other.range);
    }

    @Override
    protected int doHashCode() {
        return range.hashCode();
    }
}
