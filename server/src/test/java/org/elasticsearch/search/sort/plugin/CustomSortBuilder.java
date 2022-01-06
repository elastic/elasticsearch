/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort.plugin;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortFieldAndFormat;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Custom sort builder that just rewrites to a basic field sort
 */
public class CustomSortBuilder extends SortBuilder<CustomSortBuilder> {
    public static String NAME = "_custom";
    public static ParseField SORT_FIELD = new ParseField("sort_field");

    public final String field;
    public final SortOrder order;

    public CustomSortBuilder(String field, SortOrder order) {
        this.field = field;
        this.order = order;
    }

    public CustomSortBuilder(StreamInput in) throws IOException {
        this.field = in.readString();
        this.order = in.readOptionalWriteable(SortOrder::readFromStream);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeOptionalWriteable(order);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public SortBuilder<?> rewrite(final QueryRewriteContext ctx) throws IOException {
        return SortBuilders.fieldSort(field).order(order);
    }

    @Override
    protected SortFieldAndFormat build(final SearchExecutionContext context) throws IOException {
        throw new IllegalStateException("rewrite");
    }

    @Override
    public BucketedSort buildBucketedSort(
        final SearchExecutionContext context,
        final BigArrays bigArrays,
        final int bucketSize,
        final BucketedSort.ExtraData extra
    ) throws IOException {
        throw new IllegalStateException("rewrite");
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        CustomSortBuilder other = (CustomSortBuilder) object;
        return Objects.equals(field, other.field) && Objects.equals(order, other.order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, order);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.startObject(NAME);
        builder.field(SORT_FIELD.getPreferredName(), field);
        builder.field(ORDER_FIELD.getPreferredName(), order);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public static CustomSortBuilder fromXContent(XContentParser parser, String elementName) {
        return PARSER.apply(parser, null);
    }

    private static final ConstructingObjectParser<CustomSortBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new CustomSortBuilder((String) a[0], (SortOrder) a[1])
    );

    static {
        PARSER.declareField(constructorArg(), XContentParser::text, SORT_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), p -> SortOrder.fromString(p.text()), ORDER_FIELD, ObjectParser.ValueType.STRING);
    }
}
