/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.Strings.format;

public final class FrequentItemSetsAggregationBuilder extends AbstractAggregationBuilder<FrequentItemSetsAggregationBuilder> {

    public static final String NAME = "frequent_item_sets";

    // name used between 8.4 - 8.6, kept for backwards compatibility until 9.0
    public static final String DEPRECATED_NAME = "frequent_items";

    public static final double DEFAULT_MINIMUM_SUPPORT = 0.01;
    public static final int DEFAULT_MINIMUM_SET_SIZE = 1;
    public static final int DEFAULT_SIZE = 10;
    public static final List<String> EXECUTION_HINT_ALLOWED_MODES = List.of("global_ordinals", "map");

    public static final ParseField MINIMUM_SUPPORT = new ParseField("minimum_support");
    public static final ParseField MINIMUM_SET_SIZE = new ParseField("minimum_set_size");
    public static final ParseField FIELDS = new ParseField("fields");
    public static final ParseField EXECUTION_HINT_FIELD_NAME = new ParseField("execution_hint");

    public static final ConstructingObjectParser<FrequentItemSetsAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        (args, context) -> {
            @SuppressWarnings("unchecked")
            List<MultiValuesSourceFieldConfig> fields = (List<MultiValuesSourceFieldConfig>) args[0];
            double minimumSupport = args[1] == null ? DEFAULT_MINIMUM_SUPPORT : (double) args[1];
            int minimumSetSize = args[2] == null ? DEFAULT_MINIMUM_SET_SIZE : (int) args[2];
            int size = args[3] == null ? DEFAULT_SIZE : (int) args[3];
            QueryBuilder filter = (QueryBuilder) args[4];
            String executionHint = (String) args[5];

            return new FrequentItemSetsAggregationBuilder(context, fields, minimumSupport, minimumSetSize, size, filter, executionHint);
        }
    );

    static {
        ContextParser<Void, MultiValuesSourceFieldConfig.Builder> fieldsParser = MultiValuesSourceFieldConfig.parserBuilder(
            false,  // scriptable
            false,  // timezone aware
            false,  // filtered (not defined per field, but for all fields below)
            false,  // format
            true    // includes and excludes
        );
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, n) -> fieldsParser.parse(p, null).build(), FIELDS);
        PARSER.declareDouble(ConstructingObjectParser.optionalConstructorArg(), MINIMUM_SUPPORT);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MINIMUM_SET_SIZE);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), Aggregation.CommonFields.SIZE);
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, context) -> AbstractQueryBuilder.parseTopLevelQuery(p),
            MultiValuesSourceFieldConfig.FILTER,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), EXECUTION_HINT_FIELD_NAME);
    }

    static final ValuesSourceRegistry.RegistryKey<ItemSetMapReduceValueSource.ValueSourceSupplier> REGISTRY_KEY =
        new ValuesSourceRegistry.RegistryKey<>(NAME, ItemSetMapReduceValueSource.ValueSourceSupplier.class);

    public static void registerAggregators(ValuesSourceRegistry.Builder registry) {
        registry.registerUsage(NAME);
        registry.register(
            REGISTRY_KEY,
            List.of(CoreValuesSourceType.KEYWORD, CoreValuesSourceType.IP),
            ItemSetMapReduceValueSource.KeywordValueSource::new,
            false
        );

        registry.register(
            REGISTRY_KEY,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.NUMERIC, CoreValuesSourceType.BOOLEAN),
            ItemSetMapReduceValueSource.NumericValueSource::new,
            false
        );
    }

    private final List<MultiValuesSourceFieldConfig> fields;
    private final double minimumSupport;
    private final int minimumSetSize;
    private final int size;
    private final QueryBuilder filter;
    private final String executionHint;

    public FrequentItemSetsAggregationBuilder(
        String name,
        List<MultiValuesSourceFieldConfig> fields,
        double minimumSupport,
        int minimumSetSize,
        int size,
        QueryBuilder filter,
        String executionHint
    ) {
        super(name);
        this.fields = fields;
        if (minimumSupport <= 0.0 || minimumSupport > 1.0) {
            throw new IllegalArgumentException(
                "[minimum_support] must be greater than 0 and less or equal to 1. Found [" + minimumSupport + "] in [" + name + "]"
            );
        }
        this.minimumSupport = minimumSupport;
        if (minimumSetSize <= 0) {
            throw new IllegalArgumentException(
                "[minimum_set_size] must be greater than 0. Found [" + minimumSetSize + "] in [" + name + "]"
            );
        }
        this.minimumSetSize = minimumSetSize;
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        this.size = size;
        this.filter = filter;

        if (executionHint != null && EXECUTION_HINT_ALLOWED_MODES.contains(executionHint) == false) {
            throw new IllegalArgumentException(
                format(
                    "[execution_hint] must be one of [%s]. Found [%s]",
                    Strings.collectionToCommaDelimitedString(EXECUTION_HINT_ALLOWED_MODES),
                    executionHint
                )
            );
        }

        this.executionHint = executionHint;
    }

    public FrequentItemSetsAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.fields = in.readList(MultiValuesSourceFieldConfig::new);
        this.minimumSupport = in.readDouble();
        this.minimumSetSize = in.readVInt();
        this.size = in.readVInt();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_6_0)) {
            this.filter = in.readOptionalNamedWriteable(QueryBuilder.class);
        } else {
            this.filter = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            this.executionHint = in.readOptionalString();
        } else {
            this.executionHint = null;
        }
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new FrequentItemSetsAggregationBuilder(name, fields, minimumSupport, minimumSetSize, size, filter, executionHint);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeList(fields);
        out.writeDouble(minimumSupport);
        out.writeVInt(minimumSetSize);
        out.writeVInt(size);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_6_0)) {
            out.writeOptionalNamedWriteable(filter);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            out.writeOptionalString(executionHint);
        }
    }

    @Override
    protected AggregatorFactory doBuild(AggregationContext context, AggregatorFactory parent, Builder subfactoriesBuilder)
        throws IOException {

        return new FrequentItemSetsAggregatorFactory(
            name,
            context,
            parent,
            subfactoriesBuilder,
            metadata,
            fields,
            minimumSupport,
            minimumSetSize,
            size,
            filter,
            executionHint
        );
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(FIELDS.getPreferredName());
        for (MultiValuesSourceFieldConfig field : fields) {
            field.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(MINIMUM_SUPPORT.getPreferredName(), minimumSupport);
        builder.field(MINIMUM_SET_SIZE.getPreferredName(), minimumSetSize);
        builder.field(Aggregation.CommonFields.SIZE.getPreferredName(), size);
        if (filter != null) {
            builder.field(MultiValuesSourceFieldConfig.FILTER.getPreferredName(), filter);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    // this is a leaf only aggregation
    @Override
    public FrequentItemSetsAggregationBuilder subAggregations(Builder subFactories) {
        throw new IllegalArgumentException("Aggregator [" + name + "] of type [" + getType() + "] cannot accept sub-aggregations");
    }

    @Override
    public FrequentItemSetsAggregationBuilder subAggregation(AggregationBuilder aggregation) {
        throw new IllegalArgumentException("Aggregator [" + name + "] of type [" + getType() + "] cannot accept sub-aggregations");
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_4_0;
    }

}
