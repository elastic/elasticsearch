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

package org.elasticsearch.search.aggregations.metrics.max;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FutureArrays;
import org.elasticsearch.common.CheckedFunction;
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

import static org.elasticsearch.search.aggregations.metrics.min.MinAggregatorFactory.createEmpty;
import static org.elasticsearch.search.aggregations.metrics.min.MinAggregatorFactory.getPointReaderOrNull;

public class MaxAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource.Numeric, MaxAggregatorFactory> {

    public MaxAggregatorFactory(String name, ValuesSourceConfig<Numeric> config, SearchContext context,
            AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        return new MaxAggregator(name, null, config.format(), context, parent, pipelineAggregators, metaData, null);
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource.Numeric valuesSource, Aggregator parent,
            boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                    throws IOException {
        final Function<byte[], Number> converter = getPointReaderOrNull(context, getParent(), config);
        final CheckedFunction<LeafReader, Number, IOException> shortcutMaxFunc;
        if (converter == null) {
            shortcutMaxFunc = createEmpty();
        } else {
            final String fieldName = config.fieldContext().field();
            shortcutMaxFunc = createShortcutMax(fieldName, converter);
        }
        return new MaxAggregator(name, valuesSource, config.format(), context, parent, pipelineAggregators, metaData, shortcutMaxFunc);
    }

    /**
     * Returns a function that can extract the maximum value of a field using
     * {@link PointValues}.
     * @param fieldName The name of the field.
     * @param converter The point value converter.
     */
    public static CheckedFunction<LeafReader, Number, IOException> createShortcutMax(String fieldName, Function<byte[], Number> converter) {
        return (reader) -> {
            final PointValues pointValues = reader.getPointValues(fieldName);
            if (pointValues == null) {
                return null;
            }
            final Bits liveDocs = reader.getLiveDocs();
            if (liveDocs == null) {
                return converter.apply(pointValues.getMaxPackedValue());
            }
            int numBytes = pointValues.getBytesPerDimension();
            final byte[] maxValue = pointValues.getMaxPackedValue();
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
                            // we need to collect all values in this leaf (the sort is ascending) where
                            // the last live doc is guaranteed to contain the max value for the segment.
                            result[0] = converter.apply(packedValue);
                        }
                    }

                    @Override
                    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                        if (FutureArrays.compareUnsigned(maxValue, 0, numBytes, maxPackedValue, 0, numBytes) == 0) {
                            // we only check leaves that contain the max value for the segment.
                            return PointValues.Relation.CELL_CROSSES_QUERY;
                        } else {
                            return PointValues.Relation.CELL_OUTSIDE_QUERY;
                        }
                    }
                });
            } catch (CollectionTerminatedException e) {}
            return result[0];
        };
    }
}
