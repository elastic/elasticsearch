/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class ParsedFilters extends ParsedMultiBucketAggregation<ParsedFilters.ParsedBucket> implements Filters {

    private Map<String, ParsedBucket> bucketMap;

    @Override
    public String getType() {
        return FiltersAggregationBuilder.NAME;
    }

    @Override
    public List<? extends Filters.Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public ParsedBucket getBucketByKey(String key) {
        if (bucketMap == null) {
            bucketMap = Maps.newMapWithExpectedSize(buckets.size());
            for (ParsedBucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(key);
    }

    private static final ObjectParser<ParsedFilters, Void> PARSER = new ObjectParser<>(
        ParsedFilters.class.getSimpleName(),
        true,
        ParsedFilters::new
    );
    static {
        declareMultiBucketAggregationFields(
            PARSER,
            parser -> ParsedBucket.fromXContent(parser, false),
            parser -> ParsedBucket.fromXContent(parser, true)
        );
    }

    public static ParsedFilters fromXContent(XContentParser parser, String name) throws IOException {
        ParsedFilters aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        // in case this is not a keyed aggregation, we need to add numeric keys to the buckets
        if (aggregation.keyed == false) {
            int i = 0;
            for (ParsedBucket bucket : aggregation.buckets) {
                assert bucket.key == null;
                bucket.key = String.valueOf(i);
                i++;
            }
        }
        return aggregation;
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements Filters.Bucket {

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
            if (isKeyed()) {
                builder.startObject(key);
            } else {
                builder.startObject();
            }
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            getAggregations().toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        static ParsedBucket fromXContent(XContentParser parser, boolean keyed) throws IOException {
            final ParsedBucket bucket = new ParsedBucket();
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
                    if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
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
    }
}
