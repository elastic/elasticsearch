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

public class ParsedStringTerms extends ParsedTerms {

    @Override
    public String getType() {
        return StringTerms.NAME;
    }

    private static final ObjectParser<ParsedStringTerms, Void> PARSER = new ObjectParser<>(
        ParsedStringTerms.class.getSimpleName(),
        true,
        ParsedStringTerms::new
    );
    static {
        declareParsedTermsFields(PARSER, ParsedBucket::fromXContent);
    }

    public static ParsedStringTerms fromXContent(XContentParser parser, String name) throws IOException {
        ParsedStringTerms aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedTerms.ParsedBucket {

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
            if (key != null) {
                return key.utf8ToString();
            }
            return null;
        }

        public Number getKeyAsNumber() {
            if (key != null) {
                return Double.parseDouble(key.utf8ToString());
            }
            return null;
        }

        @Override
        protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            return builder.field(CommonFields.KEY.getPreferredName(), getKey());
        }

        static ParsedBucket fromXContent(XContentParser parser) throws IOException {
            return parseTermsBucketXContent(parser, ParsedBucket::new, (p, bucket) -> {
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
