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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.datafeed.DatafeedStats;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Contains a {@link List} of the found {@link DatafeedStats} objects and the total count found
 */
public class GetDatafeedStatsResponse extends AbstractResultResponse<DatafeedStats> {

    public static final ParseField RESULTS_FIELD = new ParseField("datafeeds");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetDatafeedStatsResponse, Void> PARSER =
        new ConstructingObjectParser<>("get_datafeed_stats_response",
            true,
            a -> new GetDatafeedStatsResponse((List<DatafeedStats>) a[0], (long) a[1]));

    static {
        PARSER.declareObjectArray(constructorArg(), DatafeedStats.PARSER, RESULTS_FIELD);
        PARSER.declareLong(constructorArg(), COUNT);
    }

    GetDatafeedStatsResponse(List<DatafeedStats> results, long count) {
        super(RESULTS_FIELD, results, count);
    }

    /**
     * The collection of {@link DatafeedStats} objects found in the query
     */
    public List<DatafeedStats> datafeedStats() {
        return results;
    }

    public static GetDatafeedStatsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public int hashCode() {
        return Objects.hash(results, count);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        GetDatafeedStatsResponse other = (GetDatafeedStatsResponse) obj;
        return Objects.equals(results, other.results) && count == other.count;
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }
}
