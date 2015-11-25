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

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.BucketMetricsFactory;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.BucketMetricsParser;

import java.text.ParseException;
import java.util.List;
import java.util.Map;


public class PercentilesBucketParser extends BucketMetricsParser {

    public static final ParseField PERCENTS = new ParseField("percents");

    @Override
    public String type() {
        return PercentilesBucketPipelineAggregator.TYPE.name();
    }

    @Override
    protected BucketMetricsFactory buildFactory(String pipelineAggregatorName, String[] bucketsPaths, Map<String, Object> unparsedParams)
            throws ParseException {

        double[] percents = null;
        int counter = 0;
        Object percentParam = unparsedParams.get(PERCENTS.getPreferredName());

        if (percentParam != null) {
            if (percentParam instanceof List) {
                percents = new double[((List) percentParam).size()];
                for (Object p : (List) percentParam) {
                    if (p instanceof Double) {
                        percents[counter] = (Double) p;
                        counter += 1;
                    } else {
                        throw new ParseException("Parameter [" + PERCENTS.getPreferredName() + "] must be an array of doubles, type `"
                                + percentParam.getClass().getSimpleName() + "` provided instead", 0);
                    }
                }
                unparsedParams.remove(PERCENTS.getPreferredName());
            } else {
                throw new ParseException("Parameter [" + PERCENTS.getPreferredName() + "] must be an array of doubles, type `"
                        + percentParam.getClass().getSimpleName() + "` provided instead", 0);
            }
        }

        PercentilesBucketPipelineAggregator.Factory factory = new PercentilesBucketPipelineAggregator.Factory(pipelineAggregatorName,
                bucketsPaths);
        if (percents != null) {
            factory.percents(percents);
        }
        return factory;
    }

    @Override
    public PipelineAggregatorFactory getFactoryPrototype() {
        return new PercentilesBucketPipelineAggregator.Factory(null, null);
    }
}
