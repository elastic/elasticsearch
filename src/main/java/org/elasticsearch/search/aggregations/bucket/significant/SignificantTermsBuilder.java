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

package org.elasticsearch.search.aggregations.bucket.significant;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;

/**
 * Creates an aggregation that finds interesting or unusual occurrences of terms in a result set.
 * 
 * This feature is marked as experimental, and may be subject to change in the future.  If you 
 * use this feature, please let us know your experience with it!
 */
public class SignificantTermsBuilder extends AggregationBuilder<SignificantTermsBuilder> {


    private String field;
    private int requiredSize = SignificantTermsParser.DEFAULT_REQUIRED_SIZE;
    private int shardSize = SignificantTermsParser.DEFAULT_SHARD_SIZE;
    private int minDocCount = SignificantTermsParser.DEFAULT_MIN_DOC_COUNT;

    public SignificantTermsBuilder(String name) {
        super(name, SignificantStringTerms.TYPE.name());
    }

    public SignificantTermsBuilder field(String field) {
        this.field = field;
        return this;
    }

    public SignificantTermsBuilder size(int requiredSize) {
        this.requiredSize = requiredSize;
        return this;
    }
    public SignificantTermsBuilder shardSize(int shardSize) {
        this.shardSize = shardSize;
        return this;
    }
    public SignificantTermsBuilder minDocCount(int minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }
        if (minDocCount != SignificantTermsParser.DEFAULT_MIN_DOC_COUNT) {
            builder.field("minDocCount", minDocCount);
        }
        if (requiredSize != SignificantTermsParser.DEFAULT_REQUIRED_SIZE) {
            builder.field("size", requiredSize);
        }
        if (shardSize != SignificantTermsParser.DEFAULT_SHARD_SIZE) {
            builder.field("shard_size", shardSize);
        }

        return builder.endObject();
    }

}
