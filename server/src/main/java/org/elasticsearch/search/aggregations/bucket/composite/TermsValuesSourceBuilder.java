/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.function.LongConsumer;
import java.util.function.LongUnaryOperator;

/**
 * A {@link CompositeValuesSourceBuilder} that builds a {@link ValuesSource} from a {@link Script} or
 * a field name.
 */
public class TermsValuesSourceBuilder extends CompositeValuesSourceBuilder<TermsValuesSourceBuilder> {
    static final String TYPE = "terms";

    private static final ObjectParser<TermsValuesSourceBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(TermsValuesSourceBuilder.TYPE);
        CompositeValuesSourceParserHelper.declareValuesSourceFields(PARSER, null);
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

    @Override
    protected CompositeValuesSourceConfig innerBuild(QueryShardContext queryShardContext, ValuesSourceConfig config) throws IOException {
        ValuesSource valuesSource = config.hasValues() ? config.getValuesSource() : null;
        if (valuesSource == null) {
            // The field is unmapped so we use a value source that can parse any type of values.
            // This is needed because the after values are parsed even when there are no values to process.
            valuesSource = ValuesSource.Bytes.WithOrdinals.EMPTY;
        }
        final MappedFieldType fieldType = config.fieldType();
        final DocValueFormat format;
        if (format() == null && fieldType instanceof DateFieldMapper.DateFieldType) {
            // defaults to the raw format on date fields (preserve timestamp as longs).
            format = DocValueFormat.RAW;
        } else {
            format = config.format();
        }
        return new CompositeValuesSourceConfig(
            name,
            fieldType,
            valuesSource,
            format,
            order(),
            missingBucket(),
            script() != null,
            (
                BigArrays bigArrays,
                IndexReader reader,
                int size,
                LongConsumer addRequestCircuitBreakerBytes,
                CompositeValuesSourceConfig compositeValuesSourceConfig) -> {

                // TODO: this is a mess, and we can do better, by knowing what actual values source we should have here.
                final int reverseMul = compositeValuesSourceConfig.reverseMul();
                if (compositeValuesSourceConfig.valuesSource() instanceof ValuesSource.Bytes.WithOrdinals
                    && reader instanceof DirectoryReader) {
                    ValuesSource.Bytes.WithOrdinals vs = (ValuesSource.Bytes.WithOrdinals) compositeValuesSourceConfig.valuesSource();
                    return new GlobalOrdinalValuesSource(
                        bigArrays,
                        compositeValuesSourceConfig.fieldType(),
                        vs::globalOrdinalsValues,
                        compositeValuesSourceConfig.format(),
                        compositeValuesSourceConfig.missingBucket(),
                        size,
                        reverseMul
                    );
                } else if (compositeValuesSourceConfig.valuesSource() instanceof ValuesSource.Bytes) {
                    ValuesSource.Bytes vs = (ValuesSource.Bytes) compositeValuesSourceConfig.valuesSource();
                    return new BinaryValuesSource(
                        bigArrays,
                        addRequestCircuitBreakerBytes,
                        compositeValuesSourceConfig.fieldType(),
                        vs::bytesValues,
                        compositeValuesSourceConfig.format(),
                        compositeValuesSourceConfig.missingBucket(),
                        size,
                        reverseMul
                    );

                } else if (compositeValuesSourceConfig.valuesSource() instanceof ValuesSource.Numeric) {
                    final ValuesSource.Numeric vs = (ValuesSource.Numeric) compositeValuesSourceConfig.valuesSource();
                    if (vs.isFloatingPoint()) {
                        return new DoubleValuesSource(
                            bigArrays,
                            compositeValuesSourceConfig.fieldType(),
                            vs::doubleValues,
                            compositeValuesSourceConfig.format(),
                            compositeValuesSourceConfig.missingBucket(),
                            size,
                            reverseMul
                        );

                    } else {
                        final LongUnaryOperator rounding;
                        if (vs instanceof RoundingValuesSource) {
                            // TODO: I'm pretty sure we can't get a RoundingValuesSource here
                            rounding = ((RoundingValuesSource) vs)::round;
                        } else {
                            rounding = LongUnaryOperator.identity();
                        }
                        return new LongValuesSource(
                            bigArrays,
                            compositeValuesSourceConfig.fieldType(),
                            vs::longValues,
                            rounding,
                            compositeValuesSourceConfig.format(),
                            compositeValuesSourceConfig.missingBucket(),
                            size,
                            reverseMul
                        );
                    }
                } else {
                    throw new IllegalArgumentException(
                        "Unknown values source type: "
                            + compositeValuesSourceConfig.valuesSource().getClass().getName()
                            + " for source: "
                            + name()
                    );
                }
            }
        );
    }
}
