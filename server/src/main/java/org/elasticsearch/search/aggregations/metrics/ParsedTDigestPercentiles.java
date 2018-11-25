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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class ParsedTDigestPercentiles extends ParsedPercentiles implements Percentiles {

    @Override
    public String getType() {
        return InternalTDigestPercentiles.NAME;
    }

    @Override
    public double percentile(double percent) {
        return getPercentile(percent);
    }

    @Override
    public String percentileAsString(double percent) {
        return getPercentileAsString(percent);
    }

    private static ObjectParser<ParsedTDigestPercentiles, Void> PARSER =
            new ObjectParser<>(ParsedTDigestPercentiles.class.getSimpleName(), true, ParsedTDigestPercentiles::new);
    static {
        ParsedPercentiles.declarePercentilesFields(PARSER);
    }

    public static ParsedTDigestPercentiles fromXContent(XContentParser parser, String name) throws IOException {
        ParsedTDigestPercentiles aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }
}
