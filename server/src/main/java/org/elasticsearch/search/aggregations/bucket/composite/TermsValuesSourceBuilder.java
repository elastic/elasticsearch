/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.List;
import java.util.function.LongConsumer;
import java.util.function.LongUnaryOperator;

/**
 * A {@link CompositeValuesSourceBuilder} that builds a {@link ValuesSource} from a {@link Script} or
 * a field name.
 */
public class TermsValuesSourceBuilder extends CompositeValuesSourceBuilder<TermsValuesSourceBuilder> {

    @FunctionalInterface
    public interface TermsCompositeSupplier {
        CompositeValuesSourceConfig apply(
            ValuesSourceConfig config,
            String name,
            boolean hasScript, // probably redundant with the config, but currently we check this two different ways...
            String format,
            MissingBucket missingBucket,
            SortOrder order
        );
    }
    static final String TYPE = "terms";
    static final ValuesSourceRegistry.RegistryKey<TermsCompositeSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
        TYPE,
        TermsCompositeSupplier.class
    );

    private static final ObjectParser<TermsValuesSourceBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(TermsValuesSourceBuilder.TYPE);
        CompositeValuesSourceParserHelper.declareValuesSourceFields(PARSER);
    }

    static TermsValuesSourceBuilder parse(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new TermsValuesSourceBuilder(name), null);
    }

    public TermsValuesSourceBuilder(String name) {
        super(name);
    }

    protected TermsValuesSourceBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {}

    @Override
    protected void doXContentBody(XContentBuilder builder, Params params) throws IOException {}

    @Override
    public String type() {
        return TYPE;
    }

    static void register(ValuesSourceRegistry.Builder builder) {
        builder.register(
            REGISTRY_KEY,
            List.of(CoreValuesSourceType.DATE, CoreValuesSourceType.NUMERIC, CoreValuesSourceType.BOOLEAN),
            (valuesSourceConfig, name, hasScript, format, missingBucket, order) -> {
                final DocValueFormat docValueFormat;
                if (format == null && valuesSourceConfig.valueSourceType() == CoreValuesSourceType.DATE) {
                    // defaults to the raw format on date fields (preserve timestamp as longs).
                    docValueFormat = DocValueFormat.RAW;
                } else {
                    docValueFormat = valuesSourceConfig.format();
                }
                return new CompositeValuesSourceConfig(
                    name,
                    valuesSourceConfig.fieldType(),
                    valuesSourceConfig.getValuesSource(),
                    docValueFormat,
                    order,
                    missingBucket,
                    hasScript,
                    (
                        BigArrays bigArrays,
                        IndexReader reader,
                        int size,
                        LongConsumer addRequestCircuitBreakerBytes,
                        CompositeValuesSourceConfig compositeValuesSourceConfig) -> {

                        final ValuesSource.Numeric vs = (ValuesSource.Numeric) compositeValuesSourceConfig.valuesSource();
                        if (vs.isFloatingPoint()) {
                            return new DoubleValuesSource(
                                bigArrays,
                                compositeValuesSourceConfig.fieldType(),
                                vs::doubleValues,
                                compositeValuesSourceConfig.format(),
                                compositeValuesSourceConfig.missingBucket(),
                                size,
                                compositeValuesSourceConfig.reverseMul()
                            );

                        } else {
                            final LongUnaryOperator rounding;
                            rounding = LongUnaryOperator.identity();
                            return new LongValuesSource(
                                bigArrays,
                                compositeValuesSourceConfig.fieldType(),
                                vs::longValues,
                                rounding,
                                compositeValuesSourceConfig.format(),
                                compositeValuesSourceConfig.missingBucket(),
                                size,
                                compositeValuesSourceConfig.reverseMul()
                            );
                        }

                    }
                );
            },
            false);

        builder.register(
            REGISTRY_KEY,
            List.of(CoreValuesSourceType.KEYWORD, CoreValuesSourceType.IP),
            (valuesSourceConfig, name, hasScript, format, missingBucket, order) -> new CompositeValuesSourceConfig(
                name,
                valuesSourceConfig.fieldType(),
                valuesSourceConfig.getValuesSource(),
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

                    if (valuesSourceConfig.hasOrdinals() && reader instanceof DirectoryReader) {
                        ValuesSource.Bytes.WithOrdinals vs = (ValuesSource.Bytes.WithOrdinals) compositeValuesSourceConfig
                            .valuesSource();
                        return new OrdinalValuesSource(
                            bigArrays,
                            addRequestCircuitBreakerBytes,
                            compositeValuesSourceConfig.fieldType(),
                            vs::ordinalsValues,
                            compositeValuesSourceConfig.format(),
                            compositeValuesSourceConfig.missingBucket(),
                            size,
                            compositeValuesSourceConfig.reverseMul()
                        );
                    } else {
                        ValuesSource.Bytes vs = (ValuesSource.Bytes) compositeValuesSourceConfig.valuesSource();
                        return new BinaryValuesSource(
                            bigArrays,
                            addRequestCircuitBreakerBytes,
                            compositeValuesSourceConfig.fieldType(),
                            vs::bytesValues,
                            compositeValuesSourceConfig.format(),
                            compositeValuesSourceConfig.missingBucket(),
                            size,
                            compositeValuesSourceConfig.reverseMul()
                        );
                    }
                }
            ),
            false);
    }

    @Override
    protected ValuesSourceType getDefaultValuesSourceType() {
        return CoreValuesSourceType.KEYWORD;
    }

    @Override
    protected CompositeValuesSourceConfig innerBuild(ValuesSourceRegistry registry, ValuesSourceConfig config) throws IOException {
        return registry.getAggregator(REGISTRY_KEY, config).apply(config, name, script() != null, format(), missing(), order());
    }
}
