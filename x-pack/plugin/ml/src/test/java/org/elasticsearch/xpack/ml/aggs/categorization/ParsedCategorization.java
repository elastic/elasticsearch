/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

class ParsedCategorization extends ParsedMultiBucketAggregation<ParsedCategorization.ParsedBucket> {

    @Override
    public String getType() {
        return CategorizeTextAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedCategorization, Void> PARSER = new ObjectParser<>(
        ParsedCategorization.class.getSimpleName(),
        true,
        ParsedCategorization::new
    );
    static {
        declareMultiBucketAggregationFields(PARSER, ParsedBucket::fromXContent, ParsedBucket::fromXContent);
    }

    public static ParsedCategorization fromXContent(XContentParser parser, String name) throws IOException {
        ParsedCategorization aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    @Override
    public List<? extends Bucket> getBuckets() {
        return buckets;
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements MultiBucketsAggregation.Bucket {

        private InternalCategorizationAggregation.BucketKey key;

        protected void setKeyAsString(String keyAsString) {
            if (keyAsString == null) {
                key = null;
                return;
            }
            if (keyAsString.isEmpty()) {
                key = new InternalCategorizationAggregation.BucketKey(new BytesRef[0]);
                return;
            }
            String[] split = Strings.tokenizeToStringArray(keyAsString, " ");
            key = new InternalCategorizationAggregation.BucketKey(
                split == null
                    ? new BytesRef[] { new BytesRef(keyAsString) }
                    : Arrays.stream(split).map(BytesRef::new).toArray(BytesRef[]::new)
            );
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key.asString();
        }

        @Override
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKey());
        }

        static InternalCategorizationAggregation.BucketKey parsedKey(final XContentParser parser) throws IOException {
            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                String toSplit = parser.text();
                String[] split = Strings.tokenizeToStringArray(toSplit, " ");
                return new InternalCategorizationAggregation.BucketKey(
                    split == null
                        ? new BytesRef[] { new BytesRef(toSplit) }
                        : Arrays.stream(split).map(BytesRef::new).toArray(BytesRef[]::new)
                );
            } else {
                return new InternalCategorizationAggregation.BucketKey(
                    XContentParserUtils.parseList(parser, p -> new BytesRef(p.binaryValue())).toArray(BytesRef[]::new)
                );
            }
        }

        static ParsedBucket fromXContent(final XContentParser parser) throws IOException {
            return ParsedMultiBucketAggregation.ParsedBucket.parseXContent(
                parser,
                false,
                ParsedBucket::new,
                (p, bucket) -> bucket.key = parsedKey(p)
            );
        }

    }

}
