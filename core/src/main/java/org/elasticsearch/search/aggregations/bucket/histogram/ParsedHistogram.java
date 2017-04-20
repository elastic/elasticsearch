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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class ParsedHistogram extends ParsedMultiBucketAggregation implements Histogram {

    private final List<ParsedBucket> buckets = new ArrayList<>();
    private boolean keyed;

    @Override
    protected String getType() {
        return HistogramAggregationBuilder.NAME;
    }

    @Override
    public List<? extends Histogram.Bucket> getBuckets() {
        return buckets;
    }

    private void setKeyed(boolean keyed) {
        this.keyed = keyed;
    }

    private void addBucket(ParsedHistogram.ParsedBucket bucket) {
        buckets.add(bucket);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (ParsedBucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    private static ObjectParser<ParsedHistogram, Void> PARSER =
            new ObjectParser<>(ParsedHistogram.class.getSimpleName(), true, ParsedHistogram::new);
    static {
        declareAggregationFields(PARSER);
        PARSER.declareField((parser, aggregation, context) -> {
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.START_OBJECT) {
                aggregation.setKeyed(true);
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    aggregation.addBucket(ParsedHistogram.ParsedBucket.fromXContent(parser, true));
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                aggregation.setKeyed(false);
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    aggregation.addBucket(ParsedHistogram.ParsedBucket.fromXContent(parser, false));
                }
            }

        }, CommonFields.BUCKETS, ObjectParser.ValueType.OBJECT_ARRAY);
    }

    public static ParsedHistogram fromXContent(XContentParser parser, String name) throws IOException {
        ParsedHistogram aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket<Double> implements Histogram.Bucket {

        static ParsedBucket fromXContent(XContentParser parser, boolean keyed) throws IOException {
            final ParsedBucket bucket = new ParsedBucket();

            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();

            if (keyed) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
                bucket.setKeyedString(currentFieldName);
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            }

            List<Aggregation> aggregations = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (CommonFields.KEY_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.setKeyAsString(parser.text());
                    } else if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        bucket.setKey(parser.doubleValue());
                    } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    String typeAndName = parser.currentName();
                    int delimiterPos = typeAndName.indexOf(Aggregation.TYPED_KEYS_DELIMITER);
                    String type;
                    String name;
                    if (delimiterPos > 0) {
                        type = typeAndName.substring(0, delimiterPos);
                        name = typeAndName.substring(delimiterPos + 1);
                        aggregations.add(parser.namedObject(Aggregation.class, type, name));
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                                "Cannot parse bucket's aggregation without type information. Set [" + RestSearchAction.TYPED_KEYS_PARAM
                                        + "] parameter on the request to ensure the type information is added to the response output");
                    }
                }
            }

            bucket.setAggregations(aggregations);
            return bucket;
        }
    }
}
