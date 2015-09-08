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
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.BucketMetricsParser;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;


public class PercentilesBucketParser extends BucketMetricsParser {

    public static final ParseField PERCENTS = new ParseField("percents");

    @Override
    public String type() {
        return PercentilesBucketPipelineAggregator.TYPE.name();
    }

    @Override
    protected PipelineAggregatorFactory buildFactory(String pipelineAggregatorName, String[] bucketsPaths, GapPolicy gapPolicy,
                                                     ValueFormatter formatter, Map<String, Object> unparsedParams) throws ParseException {

        double[] percents = new double[] { 1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0 };
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

        return new PercentilesBucketPipelineAggregator.Factory(pipelineAggregatorName, bucketsPaths, gapPolicy, formatter, percents);
    }
}
