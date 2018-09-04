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

package org.elasticsearch.search.aggregations.metrics.min;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MinAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource.Numeric, MinAggregatorFactory> {

    public MinAggregatorFactory(String name, ValuesSourceConfig<Numeric> config, SearchContext context,
            AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException {
        return new MinAggregator(name, null, config.format(), context, parent, pipelineAggregators, metaData, createEmpty());
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource.Numeric valuesSource, Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        final Function<byte[], Number> converter = getPointReaderOrNull(context, getParent(), config);
        final CheckedFunction<LeafReader, Number, IOException> shortcutMinFunc;
        if (converter == null) {
            shortcutMinFunc = createEmpty();
        } else {
            final String fieldName = config.fieldContext().field();
            shortcutMinFunc = createShortcutMin(fieldName, converter);
        }
        return new MinAggregator(name, valuesSource, config.format(), context, parent, pipelineAggregators, metaData, shortcutMinFunc);
    }

    public static CheckedFunction<LeafReader, Number, IOException> createEmpty() {
        return (c) -> null;
    }

    /**
     * Returns a converter for point values if early termination is applicable to
     * the context or null otherwise.
     *
     * @param context The {@link SearchContext} of the aggregation.
     * @param parent The parent aggregator factory.
     * @param config The config for the values source metric.
     */
    public static Function<byte[], Number> getPointReaderOrNull(SearchContext context, AggregatorFactory<?> parent,
                                                                ValuesSourceConfig<ValuesSource.Numeric> config) {
        if (context.query() != null &&
                context.query().getClass() != MatchAllDocsQuery.class) {
            return null;
        }
        if (parent != null) {
            return null;
        }
        if (config.fieldContext() != null) {
            MappedFieldType fieldType = config.fieldContext().fieldType();
            if (fieldType == null || fieldType.indexOptions() == IndexOptions.NONE) {
                return null;
            }
            Function<byte[], Number> converter = null;
            if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
                converter = ((NumberFieldMapper.NumberFieldType) fieldType)::parsePoint;
            } else if (fieldType.getClass() == DateFieldMapper.DateFieldType.class) {
                converter = (in) -> LongPoint.decodeDimension(in, 0);
            }
            return converter;
        }
        return null;
    }

    /**
     * Returns a function that can extract the minimum value of a field using
     * {@link PointValues}.
     * @param fieldName The name of the field.
     * @param converter The point value converter.
     */
    public static CheckedFunction<LeafReader, Number, IOException> createShortcutMin(String fieldName, Function<byte[], Number> converter) {
        return (reader) -> {
            final PointValues pointValues = reader.getPointValues(fieldName);
            if (pointValues == null) {
                return null;
            }
            final Bits liveDocs = reader.getLiveDocs();
            if (liveDocs == null) {
                return converter.apply(pointValues.getMinPackedValue());
            }
            final Number[] result = new Number[1];
            try {
                pointValues.intersect(new PointValues.IntersectVisitor() {
                    @Override
                    public void visit(int docID) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void visit(int docID, byte[] packedValue) {
                        if (liveDocs.get(docID)) {
                            result[0] = converter.apply(packedValue);
                            // this is the first leaf with a live doc so the value is the minimum for this segment.
                            throw new CollectionTerminatedException();
                        }
                    }

                    @Override
                    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                        return PointValues.Relation.CELL_CROSSES_QUERY;
                    }
                });
            } catch (CollectionTerminatedException e) {}
            return result[0];
        };
    }
}
