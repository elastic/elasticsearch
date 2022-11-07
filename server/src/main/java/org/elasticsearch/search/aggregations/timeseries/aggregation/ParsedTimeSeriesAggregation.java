/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.search.aggregations.bucket.terms.InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME;
import static org.elasticsearch.search.aggregations.bucket.terms.InternalTerms.SUM_OF_OTHER_DOC_COUNTS;

public class ParsedTimeSeriesAggregation extends ParsedMultiBucketAggregation<ParsedTimeSeriesAggregation.ParsedBucket>
    implements
        TimeSeriesAggregation {

    private transient Map<String, ParsedBucket> bucketMap;

    @Override
    public String getType() {
        return TimeSeriesAggregationAggregationBuilder.NAME;
    }

    @Override
    public List<? extends TimeSeriesAggregation.Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public TimeSeriesAggregation.Bucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>(buckets.size());
            for (ParsedBucket bucket : buckets) {
                bucketMap.put(bucket.getKeyAsString(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    private static final ObjectParser<ParsedTimeSeriesAggregation, Void> PARSER = new ObjectParser<>(
        ParsedTimeSeriesAggregation.class.getSimpleName(),
        true,
        ParsedTimeSeriesAggregation::new
    );
    static {
        declareMultiBucketAggregationFields(
            PARSER,
            parser -> ParsedTimeSeriesAggregation.ParsedBucket.fromXContent(parser, false),
            parser -> ParsedTimeSeriesAggregation.ParsedBucket.fromXContent(parser, true)
        );
        PARSER.declareLong((parsedTerms, value) -> parsedTerms.docCountErrorUpperBound = value, DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME);
        PARSER.declareLong((parsedTerms, value) -> parsedTerms.sumOtherDocCount = value, SUM_OF_OTHER_DOC_COUNTS);
    }

    protected long docCountErrorUpperBound;
    protected long sumOtherDocCount;

    public long getDocCountError() {
        return docCountErrorUpperBound;
    }

    public long getSumOfOtherDocCounts() {
        return sumOtherDocCount;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), getDocCountError());
        builder.field(SUM_OF_OTHER_DOC_COUNTS.getPreferredName(), getSumOfOtherDocCounts());
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (TimeSeriesAggregation.Bucket bucket : getBuckets()) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    public static ParsedTimeSeriesAggregation fromXContent(XContentParser parser, String name) throws IOException {
        ParsedTimeSeriesAggregation aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements TimeSeriesAggregation.Bucket {

        boolean showDocCountError = false;
        protected long docCountError;
        private Map<String, Object> key;
        private Map<String, Object> values;

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key.toString();
        }

        @Override
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKey());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (isKeyed()) {
                // Subclasses can override the getKeyAsString method to handle specific cases like
                // keyed bucket with RAW doc value format where the key_as_string field is not printed
                // out but we still need to have a string version of the key to use as the bucket's name.
                builder.startObject(getKeyAsString());
            } else {
                builder.startObject();
            }
            if (getKeyAsString() != null) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), getKeyAsString());
            }
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            if (showDocCountError) {
                builder.field(DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), docCountError);
            }
            builder.field(CommonFields.VALUES.getPreferredName(), values);
            getAggregations().toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        static ParsedTimeSeriesAggregation.ParsedBucket fromXContent(XContentParser parser, boolean keyed) throws IOException {
            ParsedTimeSeriesAggregation.ParsedBucket bucket = new ParsedBucket();
            bucket.setKeyed(keyed);
            String currentFieldName = parser.currentName();
            Token token = parser.currentToken();

            if (keyed) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            }

            List<Aggregation> aggregations = new ArrayList<>();
            while ((token = parser.nextToken()) != Token.END_OBJECT) {
                if (token == Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (CommonFields.KEY_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.setKeyAsString(parser.text());
                    } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
                    } else if (InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName().equals(currentFieldName)) {
                        bucket.docCountError = parser.longValue();
                        bucket.showDocCountError = true;
                    } else if (CommonFields.VALUES.getPreferredName().equals(currentFieldName)) {
                        bucket.values = new LinkedHashMap<>(parser.mapOrdered());
                    }
                } else if (token == Token.START_OBJECT) {
                    if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        bucket.key = new TreeMap<>(parser.map());
                    } else if (CommonFields.VALUES.getPreferredName().equals(currentFieldName)) {
                        bucket.values = new LinkedHashMap<>(parser.mapOrdered());
                    } else {
                        Objects.requireNonNull(aggregations);
                        XContentParserUtils.parseTypedKeysObject(parser, "#", Aggregation.class, aggregations::add);
                    }
                }
            }

            bucket.setAggregations(new Aggregations(aggregations));
            return bucket;
        }
    }

}
