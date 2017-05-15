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

package org.elasticsearch.search.aggregations.bucket.adjacency;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParsedAdjecencyMatrix extends ParsedMultiBucketAggregation<ParsedAdjecencyMatrix.ParsedBucket> implements AdjacencyMatrix {

    private Map<String, ParsedBucket> bucketMap;

    @Override
    public String getType() {
        return AdjacencyMatrixAggregationBuilder.NAME;
    }

    @Override
    public List<? extends AdjacencyMatrix.Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public ParsedBucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>(buckets.size());
            for (ParsedBucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    private static ObjectParser<ParsedAdjecencyMatrix, Void> PARSER =
            new ObjectParser<>(ParsedAdjecencyMatrix.class.getSimpleName(), true, ParsedAdjecencyMatrix::new);
    static {
        declareMultiBucketAggregationFields(PARSER,
                parser -> ParsedBucket.fromXContent(parser),
                parser -> ParsedBucket.fromXContent(parser));
    }

    public static ParsedAdjecencyMatrix fromXContent(XContentParser parser, String name) throws IOException {
        ParsedAdjecencyMatrix aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements AdjacencyMatrix.Bucket {

        private String key;

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY.getPreferredName(), key);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            getAggregations().toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }


        static ParsedBucket fromXContent(XContentParser parser) throws IOException {
            final ParsedBucket bucket = new ParsedBucket();
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();

            List<Aggregation> aggregations = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
                    } else if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        bucket.key = parser.text();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    aggregations.add(XContentParserUtils.parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, Aggregation.class));
                }
            }
            bucket.setAggregations(new Aggregations(aggregations));
            return bucket;
        }
    }
}
