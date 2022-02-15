/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FrequentItemSetsAggregationBuilder extends AbstractAggregationBuilder<FrequentItemSetsAggregationBuilder> {

    public static final String NAME = "frequent_items";

    public static final ParseField ALGORITHM_FIELD = new ParseField("algorithm");
    public static final ParseField FIELDS = new ParseField("fields");

    public static final ConstructingObjectParser<FrequentItemSetsAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        (args, context) -> {
            @SuppressWarnings("unchecked")
            List<MultiValuesSourceFieldConfig> fields = (List<MultiValuesSourceFieldConfig>) args[0];

            return new FrequentItemSetsAggregationBuilder(context, fields, (String) args[1]);
        }
    );

    static {
        ContextParser<Void, MultiValuesSourceFieldConfig.Builder> metricParser = MultiValuesSourceFieldConfig.parserBuilder(
            true,
            false,
            false,
            false
        );
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, n) -> metricParser.parse(p, null).build(), FIELDS);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ALGORITHM_FIELD);
    }

    private final String algorithm;
    private final List<MultiValuesSourceFieldConfig> fields;

    public FrequentItemSetsAggregationBuilder(String name, List<MultiValuesSourceFieldConfig> fields, String algorithm) {
        super(name);
        this.fields = fields;
        this.algorithm = algorithm != null ? algorithm : "placeholder_default";
    }

    public FrequentItemSetsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.fields = in.readList(MultiValuesSourceFieldConfig::new);
        this.algorithm = in.readString();
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new FrequentItemSetsAggregationBuilder(name, fields, algorithm);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeList(fields);
        out.writeString(algorithm);
    }

    @Override
    protected AggregatorFactory doBuild(AggregationContext context, AggregatorFactory parent, Builder subfactoriesBuilder)
        throws IOException {

        return new FrequentItemSetsAggregatorFactory(name, context, parent, subfactoriesBuilder, metadata, fields);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(FIELDS.getPreferredName());
        for (MultiValuesSourceFieldConfig field : fields) {
            field.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_2_0;
    }

}
