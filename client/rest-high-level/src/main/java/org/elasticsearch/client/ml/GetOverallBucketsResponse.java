/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.results.OverallBucket;
import org.elasticsearch.common.xcontent.ParseField;
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
