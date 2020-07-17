/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.search.builder.SearchSourceBuilder.SIZE_FIELD;
import static org.elasticsearch.search.builder.SearchSourceBuilder.SORT_FIELD;

public class TopMetricsAggregationBuilder extends AbstractAggregationBuilder<TopMetricsAggregationBuilder> {
    public static final String NAME = "top_metrics";
    public static final ParseField METRIC_FIELD = new ParseField("metrics");

    /**
     * Default to returning only a single top metric.
     */
    private static final int DEFAULT_SIZE = 1;

    public static final ConstructingObjectParser<TopMetricsAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(NAME,
            false, (args, name) -> {
                @SuppressWarnings("unchecked")
                List<SortBuilder<?>> sorts = (List<SortBuilder<?>>) args[0];
                int size = args[1] == null ? DEFAULT_SIZE : (Integer) args[1];
                if (size < 1) {
                    throw new IllegalArgumentException("[size] must be more than 0 but was [" + size + "]");
                }
                @SuppressWarnings("unchecked")
                List<MultiValuesSourceFieldConfig> metricFields = (List<MultiValuesSourceFieldConfig>) args[2];
                return new TopMetricsAggregationBuilder(name, sorts, size, metricFields);
            });
    static {
        PARSER.declareField(constructorArg(), (p, n) -> SortBuilder.fromXContent(p), SORT_FIELD,
                ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING);
        PARSER.declareInt(optionalConstructorArg(), SIZE_FIELD);
        ContextParser<Void, MultiValuesSourceFieldConfig.Builder> metricParser =
            MultiValuesSourceFieldConfig.parserBuilder(true, false, false);
        PARSER.declareObjectArray(constructorArg(), (p, n) -> metricParser.parse(p, null).build(), METRIC_FIELD);
    }

    private final List<SortBuilder<?>> sortBuilders;
    private final int size;
    private final List<MultiValuesSourceFieldConfig> metricFields;
    // TODO replace with ValuesSourceConfig once the value source refactor has landed

    /**
     * Build a {@code top_metrics} aggregation request.
     */
    public TopMetricsAggregationBuilder(String name, List<SortBuilder<?>> sortBuilders, int size,
            List<MultiValuesSourceFieldConfig> metricFields) {
        super(name);
        if (sortBuilders.size() != 1) {
            throw new IllegalArgumentException("[sort] must contain exactly one sort");
        }
        this.sortBuilders = sortBuilders;
        this.size = size;
        this.metricFields = metricFields;
    }

    /**
     * Cloning ctor for reducing.
     */
    public TopMetricsAggregationBuilder(TopMetricsAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder,
            Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.sortBuilders = clone.sortBuilders;
        this.size = clone.size;
        this.metricFields = clone.metricFields;
    }

    /**
     * Read from a stream.
     */
    public TopMetricsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        @SuppressWarnings("unchecked")
        List<SortBuilder<?>> sortBuilders = (List<SortBuilder<?>>) (List<?>) in.readNamedWriteableList(SortBuilder.class);
        this.sortBuilders = sortBuilders;
        this.size = in.readVInt();
        this.metricFields = in.readList(MultiValuesSourceFieldConfig::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(sortBuilders);
        out.writeVInt(size);
        out.writeList(metricFields);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TopMetricsAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent, Builder subFactoriesBuilder)
            throws IOException {
        return new TopMetricsAggregatorFactory(name, queryShardContext, parent, subFactoriesBuilder, metadata, sortBuilders,
                size, metricFields);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startArray(SORT_FIELD.getPreferredName());
            for (SortBuilder<?> sort : sortBuilders) {
                sort.toXContent(builder, params);
            }
            builder.endArray();
            builder.field(SIZE_FIELD.getPreferredName(), size);
            builder.startArray(METRIC_FIELD.getPreferredName());
            for (MultiValuesSourceFieldConfig metricField: metricFields) {
                metricField.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }

    List<SortBuilder<?>> getSortBuilders() {
        return sortBuilders;
    }

    int getSize() {
        return size;
    }

    List<MultiValuesSourceFieldConfig> getMetricFields() {
        return metricFields;
    }
}
