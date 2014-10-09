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

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.*;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Defines the include/exclude regular expression filtering for string terms aggregation. In this filtering logic,
 * exclusion has precedence, where the {@code include} is evaluated first and then the {@code exclude}.
 */
public class IncludeExclude {

    // The includeValue and excludeValue ByteRefs which are the result of the parsing 
    // process are converted into a LongFilter when used on numeric fields 
    // in the index.
    public static class LongFilter {
        private LongSet valids;
        private LongSet invalids;

        private LongFilter(int numValids, int numInvalids) {
            if (numValids > 0) {
                valids = new LongOpenHashSet(numValids);
            }
            if (numInvalids > 0) {
                invalids = new LongOpenHashSet(numInvalids);
            }
        }

        public boolean accept(long value) {
            return ((valids == null) || (valids.contains(value))) && ((invalids == null) || (!invalids.contains(value)));
        }

        private void addAccept(long val) {
            valids.add(val);
        }

        private void addReject(long val) {
            invalids.add(val);
        }
    }

    private final Matcher include;
    private final Matcher exclude;
    private final CharsRefBuilder scratch = new CharsRefBuilder();
    private Set<BytesRef> includeValues;
    private Set<BytesRef> excludeValues;
    private final boolean hasRegexTest;

    /**
     * @param include   The regular expression pattern for the terms to be included
     *                  (may only be {@code null} if one of the other arguments is none-null.
     * @param includeValues   The terms to be included
     *                  (may only be {@code null} if one of the other arguments is none-null.
     * @param exclude   The regular expression pattern for the terms to be excluded
     *                  (may only be {@code null} if one of the other arguments is none-null.
     * @param excludeValues   The terms to be excluded
     *                  (may only be {@code null} if one of the other arguments is none-null.
     */
    public IncludeExclude(Pattern include, Pattern exclude, Set<BytesRef> includeValues, Set<BytesRef> excludeValues) {
        assert includeValues != null || include != null || 
                exclude != null || excludeValues != null : "includes & excludes cannot both be null"; // otherwise IncludeExclude object should be null
        this.include = include != null ? include.matcher("") : null;
        this.exclude = exclude != null ? exclude.matcher("") : null;
        hasRegexTest = include != null || exclude != null;
        this.includeValues = includeValues;
        this.excludeValues = excludeValues;
    }

    /**
     * Returns whether the given value is accepted based on the {@code include} & {@code exclude} patterns.
     */
    public boolean accept(BytesRef value) {

        if (hasRegexTest) {
            // We need to perform UTF8 to UTF16 conversion for use in the regex matching
            scratch.copyUTF8Bytes(value);            
        }
        return isIncluded(value, scratch.get()) && !isExcluded(value, scratch.get());
    }
    
    private boolean isIncluded(BytesRef value, CharsRef utf16Chars) {

        if ((includeValues == null) && (include == null)) {
            // No include criteria to be tested.
            return true;
        }
        
        if (include != null) {
            if (include.reset(scratch.get()).matches()) {
                return true;
            }
        }
        if (includeValues != null) {
            if (includeValues.contains(value)) {
                return true;
            }
        }
        // Some include criteria was tested but no match found
        return false;
    }
    
    private boolean isExcluded(BytesRef value, CharsRef utf16Chars) {
        if (exclude != null) {
            if (exclude.reset(scratch.get()).matches()) {
                return true;
            }
        }
        if (excludeValues != null) {
            if (excludeValues.contains(value)) {
                return true;
            }
        }
        // No exclude criteria was tested or no match found
        return false;
    }

    /**
     * Computes which global ordinals are accepted by this IncludeExclude instance.
     */
    public LongBitSet acceptedGlobalOrdinals(RandomAccessOrds globalOrdinals, ValuesSource.Bytes.WithOrdinals valueSource) {
        LongBitSet acceptedGlobalOrdinals = new LongBitSet(globalOrdinals.getValueCount());
        // There are 3 ways of populating this bitset: 
        // 1) Looking up the global ordinals for known "include" terms
        // 2) Looking up the global ordinals for known "exclude" terms
        // 3) Traversing the term enum for all terms and running past regexes
        // Option 3 is known to be very slow in the case of high-cardinality fields and
        // should be avoided if possible.
        if (includeValues != null) {
            // optimize for the case where the set of accepted values is a set
            // of known terms, not a regex that would have to be tested against all terms in the index
            for (BytesRef includeValue : includeValues) {
                // We need to perform UTF8 to UTF16 conversion for use in the regex matching
                scratch.copyUTF8Bytes(includeValue); 
                if (!isExcluded(includeValue, scratch.get())) {
                    long ord = globalOrdinals.lookupTerm(includeValue);
                    if (ord >= 0) {
                        acceptedGlobalOrdinals.set(ord);
                    }
                }
            }
        } else {
            if(hasRegexTest) {
                // We have includeVals that are a regex or only regex excludes - we need to do the potentially 
                // slow option of hitting termsEnum for every term in the index.
                TermsEnum globalTermsEnum = valueSource.globalOrdinalsValues().termsEnum();
                try {
                    for (BytesRef term = globalTermsEnum.next(); term != null; term = globalTermsEnum.next()) {
                        if (accept(term)) {
                            acceptedGlobalOrdinals.set(globalTermsEnum.ord());
                        }
                    }
                } catch (IOException e) {
                    throw ExceptionsHelper.convertToElastic(e);
                }
            } else {
                // we only have a set of known values to exclude - create a bitset with all good values and negate the known bads
                acceptedGlobalOrdinals.set(0, acceptedGlobalOrdinals.length());
                for (BytesRef excludeValue : excludeValues) {
                    long ord = globalOrdinals.lookupTerm(excludeValue);
                    if (ord >= 0) {
                        acceptedGlobalOrdinals.clear(ord);
                    }
                }
                
            }
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
        Set<BytesRef> includeValues;
        Set<BytesRef> excludeValues;

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
            
            if (token == XContentParser.Token.START_ARRAY) {
                if ("include".equals(currentFieldName)) {
                     includeValues = parseArrayToSet(parser);
                     return true;
                } 
                if ("exclude".equals(currentFieldName)) {
                      excludeValues = parseArrayToSet(parser);
                      return true;
                }
                return false;
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
        private Set<BytesRef> parseArrayToSet(XContentParser parser) throws IOException {
            final Set<BytesRef> set = new HashSet<>();
            if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                throw new ElasticsearchParseException("Missing start of array in include/exclude clause");
            }
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (!parser.currentToken().isValue()) {
                    throw new ElasticsearchParseException("Array elements in include/exclude clauses should be string values");
                }
                set.add(new BytesRef(parser.text()));
            }
            return set;
        }
        
        public IncludeExclude includeExclude() {
            if (include == null && exclude == null && includeValues == null && excludeValues == null) {
                return null;
            }
            Pattern includePattern =  include != null ? Pattern.compile(include, includeFlags) : null;
            Pattern excludePattern = exclude != null ? Pattern.compile(exclude, excludeFlags) : null;
            return new IncludeExclude(includePattern, excludePattern, includeValues, excludeValues);
        }
    }

    public boolean isRegexBased() {
        return hasRegexTest;
    }

    public LongFilter convertToLongFilter() {
        int numValids = includeValues == null ? 0 : includeValues.size();
        int numInvalids = excludeValues == null ? 0 : excludeValues.size();
        LongFilter result = new LongFilter(numValids, numInvalids);
        if (includeValues != null) {
            for (BytesRef val : includeValues) {
                result.addAccept(Long.parseLong(val.utf8ToString()));
            }
        }
        if (excludeValues != null) {
            for (BytesRef val : excludeValues) {
                result.addReject(Long.parseLong(val.utf8ToString()));
            }
        }
        return result;
    }
    public LongFilter convertToDoubleFilter() {
        int numValids = includeValues == null ? 0 : includeValues.size();
        int numInvalids = excludeValues == null ? 0 : excludeValues.size();
        LongFilter result = new LongFilter(numValids, numInvalids);
        if (includeValues != null) {
            for (BytesRef val : includeValues) {
                double dval=Double.parseDouble(val.utf8ToString());
                result.addAccept( NumericUtils.doubleToSortableLong(dval));
            }
        }
        if (excludeValues != null) {
            for (BytesRef val : excludeValues) {
                double dval=Double.parseDouble(val.utf8ToString());
                result.addReject( NumericUtils.doubleToSortableLong(dval));
            }
        }
        return result;
    }

}
