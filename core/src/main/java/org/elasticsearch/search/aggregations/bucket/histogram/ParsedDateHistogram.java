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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class ParsedDateHistogram extends ParsedMultiBucketAggregation implements Histogram {

    @Override
    protected String getType() {
        return DateHistogramAggregationBuilder.NAME;
    }

    @Override
    public List<? extends Histogram.Bucket> getBuckets() {
        return buckets.stream().map(bucket -> (Histogram.Bucket) bucket).collect(Collectors.toList());
    }

    private static ObjectParser<ParsedDateHistogram, Void> PARSER =
            new ObjectParser<>(ParsedDateHistogram.class.getSimpleName(), true, ParsedDateHistogram::new);
    static {
        declareMultiBucketAggregationFields(PARSER,
                parser -> ParsedBucket.fromXContent(parser, false),
                parser -> ParsedBucket.fromXContent(parser, true));
    }

    public static ParsedDateHistogram fromXContent(XContentParser parser, String name) throws IOException {
        ParsedDateHistogram aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket<Long> implements Histogram.Bucket {

        @Override
        public Object getKey() {
            return new DateTime(super.getKey(), DateTimeZone.UTC);
        }

        @Override
        public String getKeyAsString() {
            String keyAsString = super.getKeyAsString();
            if (keyAsString != null) {
                return keyAsString;
            } else {
                return DocValueFormat.RAW.format((Long) super.getKey());
            }
        }

        static ParsedBucket fromXContent(XContentParser parser, boolean keyed) throws IOException {
            final ParsedBucket bucket = new ParsedBucket();
            bucket.setKeyed(keyed);

            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (keyed) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            }

            final List<Aggregation> aggregations = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (CommonFields.KEY_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.setKeyAsString(parser.text());
                    } else if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        bucket.setKey(parser.longValue());
                    } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    aggregations.add(XContentParserUtils.parseTypedKeysObject(parser, Aggregation.TYPED_KEYS_DELIMITER, Aggregation.class));
                }
            }

            bucket.setAggregations(aggregations);
            return bucket;
        }
    }
}
