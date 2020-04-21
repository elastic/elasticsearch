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

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.function.Function;

/**
 * Prepares {@link Rounding} instances for running an aggregation.
 */
public abstract class RoundingPreparer implements AggregatorSupplier {
    public static final String NAME = "RoundingPreparer";

    /**
     * Prepare the rounding for the data in the shard.
     */
    public static Function<Rounding, Rounding.Prepared> preparer(QueryShardContext context, ValuesSourceConfig config) throws IOException {
        RoundingPreparer preparer = (RoundingPreparer)
            context.getValuesSourceRegistry().getAggregator(config.valueSourceType(), RoundingPreparer.NAME);
        if (preparer == null) {
            return Rounding::prepareForUnknown;
        }
        return preparer.preparer(config, context.getIndexReader());
    }

    /**
     * Register the builtin implementations of {@link RoundingPreparer}.
     */
    public static void registerBuiltins(ValuesSourceRegistry registry) {
        registry.register(NAME, CoreValuesSourceType.DATE, new ForDate());
    }

    /**
     * Prepare the rounding for the data in the shard.
     */
    public final Function<Rounding, Rounding.Prepared> preparer(ValuesSourceConfig config, IndexReader reader) throws IOException {
        if (config.script() != null || config.missing() != null || config.fieldContext().fieldType().indexOptions() == IndexOptions.NONE) {
            return Rounding::prepareForUnknown;
        }
        return preparer(config.fieldContext().fieldType(), reader);
    }

    /**
     * The field-type-specific operations to prepare the rounding. 
     */
    protected abstract Function<Rounding, Rounding.Prepared> preparer(MappedFieldType ft, IndexReader reader) throws IOException;

    private static class ForDate extends RoundingPreparer {
        @Override
        public Function<Rounding, Rounding.Prepared> preparer(MappedFieldType ft, IndexReader reader) throws IOException {
            DateFieldType dft = (DateFieldType) ft;
            long minUtcMillis = dft.resolution().parsePointAsMillis(PointValues.getMinPackedValue(reader, ft.name()));
            long maxUtcMillis = dft.resolution().parsePointAsMillis(PointValues.getMinPackedValue(reader, ft.name()));
            return rounding -> rounding.prepare(minUtcMillis, maxUtcMillis);
        }
    }
}
