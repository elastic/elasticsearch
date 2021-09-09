/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * A {@link CompositeValuesSourceBuilder} that builds a {@link HistogramValuesSource} from another numeric values source
 * using the provided interval.
 */
public class HistogramValuesSourceBuilder extends CompositeValuesSourceBuilder<HistogramValuesSourceBuilder> {
    @FunctionalInterface
    public interface HistogramCompositeSupplier {
        CompositeValuesSourceConfig apply(
            ValuesSourceConfig config,
            double interval,
            String name,
            boolean hasScript, // probably redundant with the config, but currently we check this two different ways...
            String format,
            boolean missingBucket,
            SortOrder order
        );
    }

    static final String TYPE = "histogram";
    static final ValuesSourceRegistry.RegistryKey<HistogramCompositeSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        TYPE,
        HistogramCompositeSupplier.class
    );

    private static final ObjectParser<HistogramValuesSourceBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(HistogramValuesSourceBuilder.TYPE);
        PARSER.declareDouble(HistogramValuesSourceBuilder::interval, Histogram.INTERVAL_FIELD);
        CompositeValuesSourceParserHelper.declareValuesSourceFields(PARSER);
    }

    static HistogramValuesSourceBuilder parse(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new HistogramValuesSourceBuilder(name), null);
    }

    public static void register(ValuesSourceRegistry.Builder builder) {
        builder.register(
            REGISTRY_KEY,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.NUMERIC),
            (valuesSourceConfig, interval, name, hasScript, format, missingBucket, order) -> {
                ValuesSource.Numeric numeric = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
                final HistogramValuesSource vs = new HistogramValuesSource(numeric, interval);
                final MappedFieldType fieldType = valuesSourceConfig.fieldType();
                return new CompositeValuesSourceConfig(
                    name,
                    fieldType,
                    vs,
                    valuesSourceConfig.format(),
                    order,
                    missingBucket,
                    hasScript,
                    (
                        BigArrays bigArrays,
                        IndexReader reader,
                        int size,
                        LongConsumer addRequestCircuitBreakerBytes,
                        CompositeValuesSourceConfig compositeValuesSourceConfig) -> {
                        final ValuesSource.Numeric numericValuesSource = (ValuesSource.Numeric) compositeValuesSourceConfig.valuesSource();
                        return new DoubleValuesSource(
                            bigArrays,
                            compositeValuesSourceConfig.fieldType(),
                            numericValuesSource::doubleValues,
                            compositeValuesSourceConfig.format(),
                            compositeValuesSourceConfig.missingBucket(),
                            size,
                            compositeValuesSourceConfig.reverseMul()
                        );
                    }
                );
            },
            false
        );
    }

    private double interval = 0;

    public HistogramValuesSourceBuilder(String name) {
        super(name);
    }

    protected HistogramValuesSourceBuilder(StreamInput in) throws IOException {
        super(in);
        this.interval = in.readDouble();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(interval);
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), interval);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        HistogramValuesSourceBuilder other = (HistogramValuesSourceBuilder) obj;
        return Objects.equals(interval, other.interval);
    }

    @Override
    public String type() {
        return TYPE;
    }

    /**
     * Returns the interval that is set on this source
     **/
    public double interval() {
        return interval;
    }

    /**
     * Sets the interval on this source.
     **/
    public HistogramValuesSourceBuilder interval(double interval) {
        if (interval <= 0) {
            throw new IllegalArgumentException("[interval] must be greater than 0 for [histogram] source");
        }
        this.interval = interval;
        return this;
    }

    @Override
    protected ValuesSourceType getDefaultValuesSourceType() {
        return CoreValuesSourceType.NUMERIC;
    }

    @Override
    protected CompositeValuesSourceConfig innerBuild(ValuesSourceRegistry registry, ValuesSourceConfig config) throws IOException {
        return registry.getAggregator(REGISTRY_KEY, config)
            .apply(config, interval, name, script() != null, format(), missingBucket(), order());
    }
}
