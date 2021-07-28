/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.results.Bucket;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A response containing the requested buckets
 */
public class GetBucketsResponse extends AbstractResultResponse<Bucket> {

    public static final ParseField BUCKETS = new ParseField("buckets");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetBucketsResponse, Void> PARSER = new ConstructingObjectParser<>("get_buckets_response",
            true, a -> new GetBucketsResponse((List<Bucket>) a[0], (long) a[1]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), Bucket.PARSER, BUCKETS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), COUNT);
    }

    public static GetBucketsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    GetBucketsResponse(List<Bucket> buckets, long count) {
        super(BUCKETS, buckets, count);
    }

    /**
     * The retrieved buckets
     * @return the retrieved buckets
     */
    public List<Bucket> buckets() {
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
        GetBucketsResponse other = (GetBucketsResponse) obj;
        return count == other.count && Objects.equals(results, other.results);
    }
}
