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

import org.elasticsearch.client.ml.job.results.OverallBucket;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A response containing the requested overall buckets
 */
public class GetOverallBucketsResponse extends AbstractResultResponse<OverallBucket> {

    public static final ParseField OVERALL_BUCKETS = new ParseField("overall_buckets");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetOverallBucketsResponse, Void> PARSER = new ConstructingObjectParser<>(
            "get_overall_buckets_response", true, a -> new GetOverallBucketsResponse((List<OverallBucket>) a[0], (long) a[1]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), OverallBucket.PARSER, OVERALL_BUCKETS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), COUNT);
    }

    public static GetOverallBucketsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    GetOverallBucketsResponse(List<OverallBucket> overallBuckets, long count) {
        super(OVERALL_BUCKETS, overallBuckets, count);
    }

    /**
     * The retrieved overall buckets
     * @return the retrieved overall buckets
     */
    public List<OverallBucket> overallBuckets() {
        return results;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, results);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetOverallBucketsResponse other = (GetOverallBucketsResponse) obj;
        return count == other.count && Objects.equals(results, other.results);
    }
}
