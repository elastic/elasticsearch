/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.CheckedBiConsumer;
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

public abstract class ParsedRareTerms extends ParsedMultiBucketAggregation<ParsedRareTerms.ParsedBucket> implements RareTerms {
    @Override
    public List<? extends RareTerms.Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public RareTerms.Bucket getBucketByKey(String term) {
        for (RareTerms.Bucket bucket : getBuckets()) {
            if (bucket.getKeyAsString().equals(term)) {
                return bucket;
            }
        }
        return null;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (RareTerms.Bucket bucket : getBuckets()) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    static void declareParsedTermsFields(final ObjectParser<? extends ParsedRareTerms, Void> objectParser,
                                         final CheckedFunction<XContentParser, ParsedBucket, IOException> bucketParser) {
        declareMultiBucketAggregationFields(objectParser, bucketParser, bucketParser);
    }

    public abstract static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements RareTerms.Bucket {

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            getAggregations().toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }


        static <B extends ParsedBucket> B parseRareTermsBucketXContent(final XContentParser parser, final Supplier<B> bucketSupplier,
                                                                       final CheckedBiConsumer<XContentParser, B, IOException> keyConsumer)
            throws IOException {

            final B bucket = bucketSupplier.get();
            final List<Aggregation> aggregations = new ArrayList<>();

            XContentParser.Token token;
            String currentFieldName = parser.currentName();
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
