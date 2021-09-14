/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class ParsedRange extends ParsedMultiBucketAggregation<ParsedRange.ParsedBucket> implements Range {

    @Override
    public String getType() {
        return RangeAggregationBuilder.NAME;
    }

    @Override
    public List<? extends Range.Bucket> getBuckets() {
        return buckets;
    }

    protected static void declareParsedRangeFields(
        final ObjectParser<? extends ParsedRange, Void> objectParser,
        final CheckedFunction<XContentParser, ParsedBucket, IOException> bucketParser,
        final CheckedFunction<XContentParser, ParsedBucket, IOException> keyedBucketParser
    ) {
        declareMultiBucketAggregationFields(objectParser, bucketParser::apply, keyedBucketParser::apply);
    }

    private static final ObjectParser<ParsedRange, Void> PARSER = new ObjectParser<>(
        ParsedRange.class.getSimpleName(),
        true,
        ParsedRange::new
    );
    static {
        declareParsedRangeFields(
            PARSER,
            parser -> ParsedBucket.fromXContent(parser, false),
            parser -> ParsedBucket.fromXContent(parser, true)
        );
    }

    public static ParsedRange fromXContent(XContentParser parser, String name) throws IOException {
        ParsedRange aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements Range.Bucket {

        protected String key;
        protected double from = Double.NEGATIVE_INFINITY;
        protected String fromAsString;
        protected double to = Double.POSITIVE_INFINITY;
        protected String toAsString;

        @Override
        public String getKey() {
            return getKeyAsString();
        }

        @Override
        public String getKeyAsString() {
            String keyAsString = super.getKeyAsString();
            if (keyAsString != null) {
                return keyAsString;
            }
            return key;
        }

        @Override
        public Object getFrom() {
            return from;
        }

        @Override
        public String getFromAsString() {
            if (fromAsString != null) {
                return fromAsString;
            }
            return doubleAsString(from);
        }

        @Override
        public Object getTo() {
            return to;
        }

        @Override
        public String getToAsString() {
            if (toAsString != null) {
                return toAsString;
            }
            return doubleAsString(to);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (isKeyed()) {
                builder.startObject(key);
            } else {
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), key);
            }
            if (Double.isInfinite(from) == false) {
                builder.field(CommonFields.FROM.getPreferredName(), from);
                if (fromAsString != null) {
                    builder.field(CommonFields.FROM_AS_STRING.getPreferredName(), fromAsString);
                }
            }
            if (Double.isInfinite(to) == false) {
                builder.field(CommonFields.TO.getPreferredName(), to);
                if (toAsString != null) {
                    builder.field(CommonFields.TO_AS_STRING.getPreferredName(), toAsString);
                }
            }
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            getAggregations().toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        private static String doubleAsString(double d) {
            return Double.isInfinite(d) ? null : Double.toString(d);
        }

        protected static <B extends ParsedBucket> B parseRangeBucketXContent(
            final XContentParser parser,
            final Supplier<B> bucketSupplier,
            final boolean keyed
        ) throws IOException {
            final B bucket = bucketSupplier.get();
            bucket.setKeyed(keyed);
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = parser.currentName();
            if (keyed) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
                bucket.key = currentFieldName;
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
                        bucket.key = parser.text();
                    } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
                    } else if (CommonFields.FROM.getPreferredName().equals(currentFieldName)) {
                        bucket.from = parser.doubleValue();
                    } else if (CommonFields.FROM_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.fromAsString = parser.text();
                    } else if (CommonFields.TO.getPreferredName().equals(currentFieldName)) {
                        bucket.to = parser.doubleValue();
                    } else if (CommonFields.TO_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.toAsString = parser.text();
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    XContentParserUtils.parseTypedKeysObject(
                        parser,
                        Aggregation.TYPED_KEYS_DELIMITER,
                        Aggregation.class,
                        aggregations::add
                    );
                }
            }
            bucket.setAggregations(new Aggregations(aggregations));
            return bucket;
        }

        static ParsedBucket fromXContent(final XContentParser parser, final boolean keyed) throws IOException {
            return parseRangeBucketXContent(parser, ParsedBucket::new, keyed);
        }
    }
}
