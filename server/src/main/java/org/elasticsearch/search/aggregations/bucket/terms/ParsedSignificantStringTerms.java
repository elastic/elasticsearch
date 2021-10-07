/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.nio.CharBuffer;

public class ParsedSignificantStringTerms extends ParsedSignificantTerms {

    @Override
    public String getType() {
        return SignificantStringTerms.NAME;
    }

    private static final ObjectParser<ParsedSignificantStringTerms, Void> PARSER = new ObjectParser<>(
        ParsedSignificantStringTerms.class.getSimpleName(),
        true,
        ParsedSignificantStringTerms::new
    );
    static {
        declareParsedSignificantTermsFields(PARSER, ParsedBucket::fromXContent);
    }

    public static ParsedSignificantStringTerms fromXContent(XContentParser parser, String name) throws IOException {
        return parseSignificantTermsXContent(() -> PARSER.parse(parser, null), name);
    }

    public static class ParsedBucket extends ParsedSignificantTerms.ParsedBucket {

        private BytesRef key;

        @Override
        public Object getKey() {
            return getKeyAsString();
        }

        @Override
        public String getKeyAsString() {
            String keyAsString = super.getKeyAsString();
            if (keyAsString != null) {
                return keyAsString;
            }
            return key.utf8ToString();
        }

        public Number getKeyAsNumber() {
            return Double.parseDouble(key.utf8ToString());
        }

        @Override
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKey());
        }

        static ParsedBucket fromXContent(XContentParser parser) throws IOException {
            return parseSignificantTermsBucketXContent(parser, new ParsedBucket(), (p, bucket) -> {
                CharBuffer cb = p.charBufferOrNull();
                if (cb == null) {
                    bucket.key = null;
                } else {
                    bucket.key = new BytesRef(cb);
                }
            });
        }
    }
}
