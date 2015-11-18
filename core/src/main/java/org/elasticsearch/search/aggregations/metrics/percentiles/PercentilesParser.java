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
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.percentiles.hdr.HDRPercentilesAggregator;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.TDigestPercentilesAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;

/**
 *
 */
public class PercentilesParser extends AbstractPercentilesParser {

    public static final ParseField PERCENTS_FIELD = new ParseField("percents");

    public PercentilesParser() {
        super(true);
    }

    public final static double[] DEFAULT_PERCENTS = new double[] { 1, 5, 25, 50, 75, 95, 99 };

    @Override
    public String type() {
        return InternalTDigestPercentiles.TYPE.name();
    }

    @Override
    protected ParseField keysField() {
        return PERCENTS_FIELD;
    }

    @Override
    protected ValuesSourceAggregatorFactory<Numeric> buildFactory(String aggregationName, double[] keys, PercentilesMethod method,
            Double compression, Integer numberOfSignificantValueDigits, Boolean keyed) {
        if (method == PercentilesMethod.TDIGEST) {
            TDigestPercentilesAggregator.Factory factory = new TDigestPercentilesAggregator.Factory(aggregationName);
            if (keys != null) {
                factory.percents(keys);
            }
            if (compression != null) {
                factory.compression(compression);
            }
            if (keyed != null) {
                factory.keyed(keyed);
            }
            return factory;
        } else if (method == PercentilesMethod.HDR) {
            HDRPercentilesAggregator.Factory factory = new HDRPercentilesAggregator.Factory(aggregationName);
            if (keys != null) {
                factory.percents(keys);
            }
            if (numberOfSignificantValueDigits != null) {
                factory.numberOfSignificantValueDigits(numberOfSignificantValueDigits);
            }
            if (keyed != null) {
                factory.keyed(keyed);
            }
            return factory;
        } else {
            throw new AssertionError();
        }
    }

    @Override
    public AggregatorFactory[] getFactoryPrototypes() {
        return new AggregatorFactory[] { new TDigestPercentilesAggregator.Factory(null), new HDRPercentilesAggregator.Factory(null) };
    }

}
