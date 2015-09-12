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
package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.HDRPercentileRanksAggregator;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentileRanks;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.TDigestPercentileRanksAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class PercentileRanksParser extends AbstractPercentilesParser {

    public static final ParseField VALUES_FIELD = new ParseField("values");

    public PercentileRanksParser() {
        super(false);
    }

    @Override
    public String type() {
        return InternalTDigestPercentileRanks.TYPE.name();
    }

    @Override
    protected ParseField keysField() {
        return VALUES_FIELD;
    }

    @Override
    protected AggregatorFactory buildFactory(SearchContext context, String aggregationName, ValuesSourceConfig<Numeric> valuesSourceConfig,
            double[] keys, PercentilesMethod method, Double compression, Integer numberOfSignificantValueDigits, boolean keyed) {
        if (keys == null) {
            throw new SearchParseException(context, "Missing token values in [" + aggregationName + "].", null);
        }
        if (method == PercentilesMethod.TDIGEST) {
            return new TDigestPercentileRanksAggregator.Factory(aggregationName, valuesSourceConfig, keys, compression, keyed);
        } else if (method == PercentilesMethod.HDR) {
            return new HDRPercentileRanksAggregator.Factory(aggregationName, valuesSourceConfig, keys, numberOfSignificantValueDigits,
                    keyed);
        } else {
            throw new AssertionError();
        }
    }

}
