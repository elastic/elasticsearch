/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

// TODO: how close to the actual InternalCategorizationAggregation.Bucket class does this have to be to add any value?
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
        private String regex;
        private int maxMatchingLength;

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

        private void setRegex(String regex) {
            this.regex = regex;
        }

        private void setMaxMatchingLength(int maxMatchingLength) {
            this.maxMatchingLength = maxMatchingLength;
        }

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
            builder.startObject();
            keyToXContent(builder);
            builder.field(CategoryDefinition.REGEX.getPreferredName(), regex);
            builder.field(CategoryDefinition.MAX_MATCHING_LENGTH.getPreferredName(), maxMatchingLength);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            getAggregations().toXContentInternal(builder, params);
            builder.endObject();
            return builder;
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

        protected static ParsedBucket parseCategorizationBucketXContent(
            final XContentParser parser,
            final Supplier<ParsedBucket> bucketSupplier,
            final CheckedBiConsumer<XContentParser, ParsedBucket, IOException> keyConsumer
        ) throws IOException {
            final ParsedBucket bucket = bucketSupplier.get();
            XContentParser.Token token;
            String currentFieldName = parser.currentName();

            List<Aggregation> aggregations = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (CommonFields.KEY_AS_STRING.getPreferredName().equals(currentFieldName)) {
                        bucket.setKeyAsString(parser.text());
                    } else if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        keyConsumer.accept(parser, bucket);
                    } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                        bucket.setDocCount(parser.longValue());
                    } else if (CategoryDefinition.REGEX.getPreferredName().equals(currentFieldName)) {
                        bucket.setRegex(parser.text());
                    } else if (CategoryDefinition.MAX_MATCHING_LENGTH.getPreferredName().equals(currentFieldName)) {
                        bucket.setMaxMatchingLength(parser.intValue());
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                        keyConsumer.accept(parser, bucket);
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

        static ParsedBucket fromXContent(final XContentParser parser) throws IOException {
            return parseCategorizationBucketXContent(parser, ParsedBucket::new, (p, bucket) -> bucket.key = parsedKey(p));
        }
    }
}
