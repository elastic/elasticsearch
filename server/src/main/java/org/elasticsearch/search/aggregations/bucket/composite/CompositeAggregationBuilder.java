/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregatorFactory;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class CompositeAggregationBuilder extends AbstractAggregationBuilder<CompositeAggregationBuilder> {
    public static final String NAME = "composite";

    public static final ParseField AFTER_FIELD_NAME = new ParseField("after");
    public static final ParseField SIZE_FIELD_NAME = new ParseField("size");
    public static final ParseField SOURCES_FIELD_NAME = new ParseField("sources");

    public static final ConstructingObjectParser<CompositeAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
            NAME, false, (args, name) -> {
                @SuppressWarnings("unchecked")
                List<CompositeValuesSourceBuilder<?>> sources = (List<CompositeValuesSourceBuilder<?>>) args[0];
                return new CompositeAggregationBuilder(name, sources);
            });
    static {
        PARSER.declareObjectArray(constructorArg(),
            (p, c) -> CompositeValuesSourceParserHelper.fromXContent(p), SOURCES_FIELD_NAME);
        PARSER.declareInt(CompositeAggregationBuilder::size, SIZE_FIELD_NAME);
        PARSER.declareObject(CompositeAggregationBuilder::aggregateAfter, (p, context) -> p.map(), AFTER_FIELD_NAME);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        DateHistogramValuesSourceBuilder.register(builder);
        HistogramValuesSourceBuilder.register(builder);
        GeoTileGridValuesSourceBuilder.register(builder);
        TermsValuesSourceBuilder.register(builder);
        builder.registerUsage(NAME);
    }

    private List<CompositeValuesSourceBuilder<?>> sources;
    private Map<String, Object> after;
    private int size = 10;

    public CompositeAggregationBuilder(String name, List<CompositeValuesSourceBuilder<?>> sources) {
        super(name);
        validateSources(sources);
        this.sources = sources;
    }

    protected CompositeAggregationBuilder(CompositeAggregationBuilder clone,
                                          AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.sources = new ArrayList<>(clone.sources);
        this.after = clone.after;
        this.size = clone.size;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new CompositeAggregationBuilder(this, factoriesBuilder, metadata);
    }

    public CompositeAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        int num = in.readVInt();
        this.sources = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            CompositeValuesSourceBuilder<?> builder = CompositeValuesSourceParserHelper.readFrom(in);
            sources.add(builder);
        }
        this.size = in.readVInt();
        if (in.readBoolean()) {
            this.after = in.readMap();
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(sources.size());
        for (CompositeValuesSourceBuilder<?> builder : sources) {
            CompositeValuesSourceParserHelper.writeTo(builder, out);
        }
        out.writeVInt(size);
        out.writeBoolean(after != null);
        if (after != null) {
            out.writeMap(after);
        }
    }

    @Override
    public String getType() {
        return NAME;
    }

    /**
     * Gets the list of {@link CompositeValuesSourceBuilder} for this aggregation.
     */
    public List<CompositeValuesSourceBuilder<?>> sources() {
        return sources;
    }

    /**
     * Sets the values that indicates which composite bucket this request should "aggregate after".
     * Defaults to {@code null}.
     */
    public CompositeAggregationBuilder aggregateAfter(Map<String, Object> afterKey) {
        this.after = afterKey;
        return this;
    }

    /**
     * The number of composite buckets to return. Defaults to {@code 10}.
     */
    public CompositeAggregationBuilder size(int size) {
        this.size = size;
        return this;
    }

    /**
     * @return the number of composite buckets. Defaults to {@code 10}.
     */
    public int size() {
        return size;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        /*
         * Cardinality *does* have buckets so MULTI might be appropriate here.
         * But the buckets can't be used with the composite agg so we're
         * going to pretend that it doesn't have buckets.
         */
        return BucketCardinality.NONE;
    }

    /**
     * Returns null if the provided factory and his parents are compatible with
     * this aggregator or the instance of the parent's factory that is incompatible with
     * the composite aggregation.
     */
    private AggregatorFactory validateParentAggregations(AggregatorFactory factory) {
        if (factory == null) {
            return null;
        } else if (factory instanceof NestedAggregatorFactory || factory instanceof FilterAggregatorFactory) {
            return validateParentAggregations(factory.getParent());
        } else {
            return factory;
        }
    }

    private static void validateSources(List<CompositeValuesSourceBuilder<?>> sources) {
        if (sources == null || sources.isEmpty()) {
            throw new IllegalArgumentException("Composite [" + SOURCES_FIELD_NAME.getPreferredName() + "] cannot be null or empty");
        }

        Set<String> names = new HashSet<>();
        Set<String> duplicates = new HashSet<>();
        sources.forEach(source -> {
            if (source == null) {
                throw new IllegalArgumentException("Composite source cannot be null");
            }
            boolean unique = names.add(source.name());
            if (unique == false) {
                duplicates.add(source.name());
            }
        });

        if (duplicates.size() > 0) {
            throw new IllegalArgumentException("Composite source names must be unique, found duplicates: " + duplicates);
        }
    }

    @Override
    protected AggregatorFactory doBuild(AggregationContext context, AggregatorFactory parent,
                                        AggregatorFactories.Builder subfactoriesBuilder) throws IOException {
        AggregatorFactory invalid = validateParentAggregations(parent);
        if (invalid != null) {
            throw new IllegalArgumentException("[composite] aggregation cannot be used with a parent aggregation of" +
                " type: [" + invalid.getClass().getSimpleName() + "]");
        }
        CompositeValuesSourceConfig[] configs = new CompositeValuesSourceConfig[sources.size()];
        for (int i = 0; i < configs.length; i++) {
            configs[i] = sources.get(i).build(context);
            if (configs[i].valuesSource().needsScores()) {
                throw new IllegalArgumentException("[sources] cannot access _score");
            }
        }
        final CompositeKey afterKey;
        if (after != null) {
            if (after.size() != configs.length) {
                throw new IllegalArgumentException("[after] has " + after.size() +
                    " value(s) but [sources] has " + sources.size());
            }
            @SuppressWarnings("rawtypes")
            Comparable<?>[] values = new Comparable[sources.size()];
            for (int i = 0; i < sources.size(); i++) {
                String sourceName = sources.get(i).name();
                if (after.containsKey(sourceName) == false) {
                    throw new IllegalArgumentException("Missing value for [after." + sources.get(i).name() + "]");
                }
                Object obj = after.get(sourceName);
                if (configs[i].missingBucket() && obj == null) {
                    values[i] = null;
                } else if (obj instanceof Comparable) {
                    values[i] = (Comparable<?>) obj;
                } else {
                    throw new IllegalArgumentException("Invalid value for [after." + sources.get(i).name() +
                        "], expected comparable, got [" + (obj == null ? "null" :  obj.getClass().getSimpleName()) + "]");
                }
            }
            afterKey = new CompositeKey(values);
        } else {
            afterKey = null;
        }
        return new CompositeAggregationFactory(name, context, parent, subfactoriesBuilder, metadata, size, configs, afterKey);
    }


    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SIZE_FIELD_NAME.getPreferredName(), size);
        builder.startArray(SOURCES_FIELD_NAME.getPreferredName());
        for (CompositeValuesSourceBuilder<?> source: sources) {
            CompositeValuesSourceParserHelper.toXContent(source, builder, params);
        }
        builder.endArray();
        if (after != null) {
            CompositeAggregation.buildCompositeMap(AFTER_FIELD_NAME.getPreferredName(), after, builder);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sources, size, after);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        CompositeAggregationBuilder other = (CompositeAggregationBuilder) obj;
        return size == other.size &&
            Objects.equals(sources, other.sources) &&
            Objects.equals(after, other.after);
    }
}
