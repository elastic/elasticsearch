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

import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class PercentileRanksParser extends AbstractPercentilesParser {

    public PercentileRanksParser() {
        super(false);
    }

    @Override
    public String type() {
        return InternalPercentileRanks.TYPE.name();
    }

    @Override
    protected String keysFieldName() {
        return "values";
    }
    
    @Override
    protected AggregatorFactory buildFactory(SearchContext context, String aggregationName, ValuesSourceConfig<Numeric> valuesSourceConfig, double[] keys, double compression, boolean keyed) {
        if (keys == null) {
            throw new SearchParseException(context, "Missing token values in [" + aggregationName + "].");
        }
        return new PercentileRanksAggregator.Factory(aggregationName, valuesSourceConfig, keys, compression, keyed);
    }

}
