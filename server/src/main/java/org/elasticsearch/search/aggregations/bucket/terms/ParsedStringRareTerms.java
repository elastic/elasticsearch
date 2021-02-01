/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import java.io.IOException;
import java.nio.CharBuffer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

public class ParsedStringRareTerms extends ParsedRareTerms {
    @Override
    public String getType() {
        return StringRareTerms.NAME;
    }

    private static final ObjectParser<ParsedStringRareTerms, Void> PARSER =
        new ObjectParser<>(ParsedStringRareTerms.class.getSimpleName(), true, ParsedStringRareTerms::new);

    static {
        declareParsedTermsFields(PARSER, ParsedBucket::fromXContent);
    }

    public static ParsedStringRareTerms fromXContent(XContentParser parser, String name) throws IOException {
        ParsedStringRareTerms aggregation = PARSER.parse(parser, null);
        aggregation.setName(name);
        return aggregation;
    }

    public static class ParsedBucket extends ParsedRareTerms.ParsedBucket {

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

        static ParsedStringRareTerms.ParsedBucket fromXContent(XContentParser parser) throws IOException {
            return parseRareTermsBucketXContent(parser, ParsedStringRareTerms.ParsedBucket::new, (p, bucket) -> {
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
