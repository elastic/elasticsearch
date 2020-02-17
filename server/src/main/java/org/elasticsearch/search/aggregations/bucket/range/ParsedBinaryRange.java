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

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class ParsedBinaryRange extends ParsedMultiBucketAggregation<ParsedBinaryRange.ParsedBucket> implements Range {

    @Override
    public String getType() {
        return IpRangeAggregationBuilder.NAME;
    }

    @Override
    public List<? extends Range.Bucket> getBuckets() {
        return buckets;
    }

    private static final ObjectParser<ParsedBinaryRange, Void> PARSER =
            new ObjectParser<>(ParsedBinaryRange.class.getSimpleName(), true, ParsedBinaryRange::new);
    static {
        declareMultiBucketAggregationFields(PARSER,
                parser -> ParsedBucket.fromXContent(parser, false),
                parser -> ParsedBucket.fromXContent(parser, true));
    }

    public static ParsedBinaryRange fromXContent(XContentParser parser, String name) throws IOException {
        ParsedBinaryRange aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements Range.Bucket {

        private String key;
        private String from;
        private String to;

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key;
        }

        @Override
        public Object getFrom() {
            return from;
        }

        @Override
        public String getFromAsString() {
            return from;
        }

        @Override
        public Object getTo() {
            return to;
        }

        @Override
        public String getToAsString() {
            return to;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (isKeyed()) {
                builder.startObject(key);
            } else {
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), key);
            }
            if (from != null) {
                builder.field(CommonFields.FROM.getPreferredName(), from);
            }
            if (to != null) {
                builder.field(CommonFields.TO.getPreferredName(), to);
            }
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            getAggregations().toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        static ParsedBucket fromXContent(final XContentParser parser, final boolean keyed) throws IOException {
            final ParsedBucket bucket = new ParsedBucket();
            bucket.setKeyed(keyed);
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();

            if (keyed) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
                bucket.key = currentFieldName;
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            }

            List<Aggregation> aggregations = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        bucket.key = parser.text();
                    } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
                    } else if (CommonFields.FROM.getPreferredName().equals(currentFieldName)) {
                        bucket.from = parser.text();
                    } else if (CommonFields.TO.getPreferredName().equals(currentFieldName)) {
                        bucket.to = parser.text();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    XContentParserUtils.parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, Aggregation.class,
                            aggregations::add);
                }
            }
            bucket.setAggregations(new Aggregations(aggregations));
            return bucket;
        }
    }
}
