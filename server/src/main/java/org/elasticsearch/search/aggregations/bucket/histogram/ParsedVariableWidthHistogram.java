/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

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

public class ParsedVariableWidthHistogram extends ParsedMultiBucketAggregation<ParsedVariableWidthHistogram.ParsedBucket>
    implements
        Histogram {

    @Override
    public String getType() {
        return VariableWidthHistogramAggregationBuilder.NAME;
    }

    @Override
    public List<? extends Histogram.Bucket> getBuckets() {
        return buckets;
    }

    private static ObjectParser<ParsedVariableWidthHistogram, Void> PARSER = new ObjectParser<>(
        ParsedVariableWidthHistogram.class.getSimpleName(),
        true,
        ParsedVariableWidthHistogram::new
    );
    static {
        declareMultiBucketAggregationFields(
            PARSER,
            parser -> ParsedBucket.fromXContent(parser, false),
            parser -> ParsedBucket.fromXContent(parser, true)
        );
    }

    public static ParsedVariableWidthHistogram fromXContent(XContentParser parser, String name) throws IOException {
        ParsedVariableWidthHistogram aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements Histogram.Bucket {
        private Double key;

        private Double min;
        private Double max;

        private String minAsString;
        private String maxAsString;

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            String keyAsString = super.getKeyAsString();
            if (keyAsString != null) {
                return keyAsString;
            }
            if (key != null) {
                return Double.toString(key);
            }
            return null;
        }

        public void setMin(Double min) {
            this.min = min;
        }

        public void setMinAsString(String minAsString) {
            this.minAsString = minAsString;
        }

        public double getMin() {
            return min;
        }

        public String getMinAsString() {
            if (minAsString != null) {
                return minAsString;
            }
            if (min != null) {
                return Double.toString(min);
            }
            return null;
        }

        public void setMax(Double max) {
            this.max = max;
        }

        public void setMaxAsString(String maxAsString) {
            this.maxAsString = maxAsString;
        }

        public double getMax() {
            return max;
        }

        public String getMaxAsString() {
            if (maxAsString != null) {
                return maxAsString;
            }
            if (max != null) {
                return Double.toString(max);
            }
            return null;
        }

        static ParsedBucket fromXContent(XContentParser parser, boolean keyed) throws IOException {
            final ParsedBucket bucket = new ParsedBucket();
            bucket.setKeyed(keyed);
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (keyed) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            }

            List<Aggregation> aggregations = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (CommonFields.KEY_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.setKeyAsString(parser.text());
                    } else if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        bucket.key = parser.doubleValue();
                    } else if (CommonFields.MIN_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.setMinAsString(parser.text());
                    } else if (CommonFields.MIN.getPreferredName().equals(currentFieldName)) {
                        bucket.setMin(parser.doubleValue());
                    } else if (CommonFields.MAX_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.setMaxAsString(parser.text());
                    } else if (CommonFields.MAX.getPreferredName().equals(currentFieldName)) {
                        bucket.setMax(parser.doubleValue());
                    } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        bucket.key = parser.doubleValue();
                    } else {
                        XContentParserUtils.parseTypedKeysObject(
                            parser,
                            Aggregation.TYPED_KEYS_DELIMITER,
                            Aggregation.class,
                            aggregations::add
                        );
                    }
                }
            }
            bucket.setAggregations(new Aggregations(aggregations));
            return bucket;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (isKeyed()) {
                builder.startObject(getKeyAsString());
            } else {
                builder.startObject();
            }

            if (minAsString != null) {
                builder.field(CommonFields.MIN_AS_STRING.getPreferredName(), minAsString);
            }
            builder.field(CommonFields.MIN.getPreferredName(), getMin());

            if (super.getKeyAsString() != null) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), getKeyAsString());
            }
            keyToXContent(builder);

            if (maxAsString != null) {
                builder.field(CommonFields.MAX_AS_STRING.getPreferredName(), maxAsString);
            }
            builder.field(CommonFields.MAX.getPreferredName(), getMax());

            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            getAggregations().toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }
    }
}
