/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ParsedComposite extends ParsedMultiBucketAggregation<ParsedComposite.ParsedBucket> implements CompositeAggregation {
    private static final ObjectParser<ParsedComposite, Void> PARSER = new ObjectParser<>(
        ParsedComposite.class.getSimpleName(),
        true,
        ParsedComposite::new
    );

    static {
        PARSER.declareField(
            ParsedComposite::setAfterKey,
            (p, c) -> p.mapOrdered(),
            new ParseField("after_key"),
            ObjectParser.ValueType.OBJECT
        );
        declareMultiBucketAggregationFields(PARSER, parser -> ParsedComposite.ParsedBucket.fromXContent(parser), parser -> null);
    }

    private Map<String, Object> afterKey;

    public static ParsedComposite fromXContent(XContentParser parser, String name) throws IOException {
        ParsedComposite aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        if (aggregation.afterKey == null && aggregation.getBuckets().size() > 0) {
            /**
             * Previous versions (< 6.3) don't send <code>afterKey</code>
             * in the response so we set it as the last returned buckets.
             */
            aggregation.setAfterKey(aggregation.getBuckets().get(aggregation.getBuckets().size() - 1).key);
        }
        return aggregation;
    }

    @Override
    public String getType() {
        return CompositeAggregationBuilder.NAME;
    }

    @Override
    public List<ParsedBucket> getBuckets() {
        return buckets;
    }

    @Override
    public Map<String, Object> afterKey() {
        if (afterKey != null) {
            return afterKey;
        }
        return buckets.size() > 0 ? buckets.get(buckets.size() - 1).getKey() : null;
    }

    private void setAfterKey(Map<String, Object> afterKey) {
        this.afterKey = afterKey;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return CompositeAggregation.toXContentFragment(this, builder, params);
    }

    public static class ParsedBucket extends ParsedMultiBucketAggregation.ParsedBucket implements CompositeAggregation.Bucket {
        private Map<String, Object> key;

        @Override
        public String getKeyAsString() {
            return key.toString();
        }

        @Override
        public Map<String, Object> getKey() {
            return key;
        }

        void setKey(Map<String, Object> key) {
            this.key = key;
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            /**
             * See {@link CompositeAggregation#bucketToXContent}
             */
            throw new UnsupportedOperationException("not implemented");
        }

        static ParsedComposite.ParsedBucket fromXContent(XContentParser parser) throws IOException {
            return parseXContent(parser, false, ParsedBucket::new, (p, bucket) -> bucket.setKey(p.mapOrdered()));
        }
    }
}
