/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.aggregations.pipeline;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.GAP_POLICY;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Builds a pipeline aggregation that allows sorting the buckets of its parent
 * aggregation. The bucket {@code _key}, {@code _count} or sub-aggregations may be used as sort
 * keys. Parameters {@code from} and {@code size} may also be set in order to truncate the
 * result bucket list.
 */
public class BucketSortPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<BucketSortPipelineAggregationBuilder> {
    public static final String NAME = "bucket_sort";

    private static final ParseField FROM = new ParseField("from");
    private static final ParseField SIZE = new ParseField("size");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<BucketSortPipelineAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        (a, context) -> new BucketSortPipelineAggregationBuilder(context, (List<FieldSortBuilder>) a[0])
    );

    static {
        PARSER.declareField(optionalConstructorArg(), (p, c) -> {
            List<SortBuilder<?>> sorts = SortBuilder.fromXContent(p);
            List<FieldSortBuilder> fieldSorts = new ArrayList<>(sorts.size());
            for (SortBuilder<?> sort : sorts) {
                if (sort instanceof FieldSortBuilder == false) {
                    throw new IllegalArgumentException(
                        "[" + NAME + "] only supports field based sorting; incompatible sort: [" + sort + "]"
                    );
                }
                fieldSorts.add((FieldSortBuilder) sort);
            }
            return fieldSorts;
        }, SearchSourceBuilder.SORT_FIELD, ObjectParser.ValueType.OBJECT_ARRAY);
        PARSER.declareInt(BucketSortPipelineAggregationBuilder::from, FROM);
        PARSER.declareInt(BucketSortPipelineAggregationBuilder::size, SIZE);
        PARSER.declareField(BucketSortPipelineAggregationBuilder::gapPolicy, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return GapPolicy.parse(p.text().toLowerCase(Locale.ROOT), p.getTokenLocation());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, GAP_POLICY, ObjectParser.ValueType.STRING);
    }

    private List<FieldSortBuilder> sorts = Collections.emptyList();
    private int from = 0;
    private Integer size;
    private GapPolicy gapPolicy = GapPolicy.SKIP;

    public BucketSortPipelineAggregationBuilder(String name, List<FieldSortBuilder> sorts) {
        super(name, NAME, sorts == null ? new String[0] : sorts.stream().map(FieldSortBuilder::getFieldName).toArray(String[]::new));
        this.sorts = sorts == null ? Collections.emptyList() : sorts;
    }

    /**
     * Read from a stream.
     */
    public BucketSortPipelineAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        sorts = in.readCollectionAsList(FieldSortBuilder::new);
        from = in.readVInt();
        size = in.readOptionalVInt();
        gapPolicy = GapPolicy.readFrom(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeCollection(sorts);
        out.writeVInt(from);
        out.writeOptionalVInt(size);
        gapPolicy.writeTo(out);
    }

    public BucketSortPipelineAggregationBuilder from(int from) {
        if (from < 0) {
            throw new IllegalArgumentException("[" + FROM.getPreferredName() + "] must be a non-negative integer: [" + from + "]");
        }
        this.from = from;
        return this;
    }

    public BucketSortPipelineAggregationBuilder size(Integer size) {
        if (size != null && size <= 0) {
            throw new IllegalArgumentException("[" + SIZE.getPreferredName() + "] must be a positive integer: [" + size + "]");
        }
        this.size = size;
        return this;
    }

    public BucketSortPipelineAggregationBuilder gapPolicy(GapPolicy gapPolicy) {
        if (gapPolicy == null) {
            throw new IllegalArgumentException("[" + GAP_POLICY.getPreferredName() + "] must not be null: [" + name + "]");
        }
        this.gapPolicy = gapPolicy;
        return this;
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metadata) {
        return new BucketSortPipelineAggregator(name, sorts, from, size, gapPolicy, metadata);
    }

    @Override
    protected void validate(ValidationContext context) {
        context.validateHasParent(NAME, name);
        if (sorts.isEmpty() && size == null && from == 0) {
            context.addValidationError(
                "["
                    + name
                    + "] is configured to perform nothing. Please set either of "
                    + Arrays.asList(SearchSourceBuilder.SORT_FIELD.getPreferredName(), SIZE.getPreferredName(), FROM.getPreferredName())
                    + " to use "
                    + NAME
            );
        }
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.xContentList(SearchSourceBuilder.SORT_FIELD.getPreferredName(), sorts);
        builder.field(FROM.getPreferredName(), from);
        if (size != null) {
            builder.field(SIZE.getPreferredName(), size);
        }
        builder.field(GAP_POLICY.getPreferredName(), gapPolicy);
        return builder;
    }

    public static BucketSortPipelineAggregationBuilder parse(String reducerName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, reducerName);
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sorts, from, size, gapPolicy);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        BucketSortPipelineAggregationBuilder other = (BucketSortPipelineAggregationBuilder) obj;
        return Objects.equals(sorts, other.sorts)
            && Objects.equals(from, other.from)
            && Objects.equals(size, other.size)
            && Objects.equals(gapPolicy, other.gapPolicy);
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
