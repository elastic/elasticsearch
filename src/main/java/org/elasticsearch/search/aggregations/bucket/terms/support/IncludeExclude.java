/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket.terms.support;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Defines the include/exclude regular expression filtering for string terms aggregation. In this filtering logic,
 * exclusion has precedence, where the {@code include} is evaluated first and then the {@code exclude}.
 */
public class IncludeExclude {

    private final Matcher include;
    private final Matcher exclude;
    private final CharsRef scratch = new CharsRef();

    /**
     * @param include   The regular expression pattern for the terms to be included
     *                  (may only be {@code null} if {@code exclude} is not {@code null}
     * @param exclude   The regular expression pattern for the terms to be excluded
     *                  (may only be {@code null} if {@code include} is not {@code null}
     */
    public IncludeExclude(Pattern include, Pattern exclude) {
        assert include != null || exclude != null : "include & exclude cannot both be null"; // otherwise IncludeExclude object should be null
        this.include = include != null ? include.matcher("") : null;
        this.exclude = exclude != null ? exclude.matcher("") : null;
    }

    /**
     * Returns whether the given value is accepted based on the {@code include} & {@code exclude} patterns.
     */
    public boolean accept(BytesRef value) {
        UnicodeUtil.UTF8toUTF16(value, scratch);
        if (include == null) {
            // exclude must not be null
            return !exclude.reset(scratch).matches();
        }
        if (!include.reset(scratch).matches()) {
            return false;
        }
        if (exclude == null) {
            return true;
        }
        return !exclude.reset(scratch).matches();
    }

    /**
     * Computes which global ordinals are accepted by this IncludeExclude instance.
     */
    public LongBitSet acceptedGlobalOrdinals(BytesValues.WithOrdinals globalOrdinals, ValuesSource.Bytes.WithOrdinals valueSource) {
        TermsEnum globalTermsEnum = valueSource.globalBytesValues().getTermsEnum();
        LongBitSet acceptedGlobalOrdinals = new LongBitSet(globalOrdinals.getMaxOrd());
        try {
            for (BytesRef term = globalTermsEnum.next(); term != null; term = globalTermsEnum.next()) {
                if (accept(term)) {
                    acceptedGlobalOrdinals.set(globalTermsEnum.ord());
                }
            }
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
        return acceptedGlobalOrdinals;
    }

    public static class Parser {

        private final String aggName;
        private final InternalAggregation.Type aggType;
        private final SearchContext context;

        String include = null;
        int includeFlags = 0; // 0 means no flags
        String exclude = null;
        int excludeFlags = 0; // 0 means no flags

        public Parser(String aggName, InternalAggregation.Type aggType, SearchContext context) {
            this.aggName = aggName;
            this.aggType = aggType;
            this.context = context;
        }

        public boolean token(String currentFieldName, XContentParser.Token token, XContentParser parser) throws IOException {

            if (token == XContentParser.Token.VALUE_STRING) {
                if ("include".equals(currentFieldName)) {
                    include = parser.text();
                } else if ("exclude".equals(currentFieldName)) {
                    exclude = parser.text();
                } else {
                    return false;
                }
                return true;
            }

            if (token == XContentParser.Token.START_OBJECT) {
                if ("include".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if ("pattern".equals(currentFieldName)) {
                                include = parser.text();
                            } else if ("flags".equals(currentFieldName)) {
                                includeFlags = Regex.flagsFromString(parser.text());
                            }
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if ("flags".equals(currentFieldName)) {
                                includeFlags = parser.intValue();
                            }
                        }
                    }
                } else if ("exclude".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if ("pattern".equals(currentFieldName)) {
                                exclude = parser.text();
                            } else if ("flags".equals(currentFieldName)) {
                                excludeFlags = Regex.flagsFromString(parser.text());
                            }
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if ("flags".equals(currentFieldName)) {
                                excludeFlags = parser.intValue();
                            }
                        }
                    }
                } else {
                    return false;
                }
                return true;
            }

            return false;
        }

        public IncludeExclude includeExclude() {
            if (include == null && exclude == null) {
                return null;
            }
            Pattern includePattern =  include != null ? Pattern.compile(include, includeFlags) : null;
            Pattern excludePattern = exclude != null ? Pattern.compile(exclude, excludeFlags) : null;
            return new IncludeExclude(includePattern, excludePattern);
        }
    }

}
