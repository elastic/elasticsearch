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

package org.elasticsearch.index.mapper;

import com.google.common.base.Strings;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public interface FieldMapper extends Mapper {

    String DOC_VALUES_FORMAT = "doc_values_format";

    class Names {

        private final String shortName;

        private final String indexName;

        private final String originalIndexName;

        private final String fullName;

        public Names(String name) {
            this(name, name, name, name);
        }

        public Names(String shortName, String indexName, String originalIndexName, String fullName) {
            this.shortName = shortName;
            this.indexName = indexName;
            this.originalIndexName = originalIndexName;
            this.fullName = fullName;
        }

        /**
         * The logical name of the field.
         */
        public String shortName() {
            return shortName;
        }

        /**
         * The indexed name of the field. This is the name under which we will
         * store it in the index.
         */
        public String indexName() {
            return indexName;
        }

        /**
         * The original index name, before any "path" modifications performed on it.
         */
        public String originalIndexName() {
            return originalIndexName;
        }

        /**
         * The full name, including dot path.
         */
        public String fullName() {
            return fullName;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;

            Names names = (Names) o;

            if (!fullName.equals(names.fullName)) return false;
            if (!indexName.equals(names.indexName)) return false;
            if (!originalIndexName.equals(names.originalIndexName)) return false;
            if (!shortName.equals(names.shortName)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = shortName.hashCode();
            result = 31 * result + indexName.hashCode();
            result = 31 * result + originalIndexName.hashCode();
            result = 31 * result + fullName.hashCode();
            return result;
        }
    }

    enum Loading {
        LAZY {
            @Override
            public String toString() {
                return LAZY_VALUE;
            }
        },
        EAGER {
            @Override
            public String toString() {
                return EAGER_VALUE;
            }
        },
        EAGER_GLOBAL_ORDINALS {
            @Override
            public String toString() {
                return EAGER_GLOBAL_ORDINALS_VALUE;
            }
        };

        public static final String KEY = "loading";
        public static final String EAGER_GLOBAL_ORDINALS_VALUE = "eager_global_ordinals";
        public static final String EAGER_VALUE = "eager";
        public static final String LAZY_VALUE = "lazy";

        public static Loading parse(String loading, Loading defaultValue) {
            if (Strings.isNullOrEmpty(loading)) {
                return defaultValue;
            } else if (EAGER_GLOBAL_ORDINALS_VALUE.equalsIgnoreCase(loading)) {
                return EAGER_GLOBAL_ORDINALS;
            } else if (EAGER_VALUE.equalsIgnoreCase(loading)) {
                return EAGER;
            } else if (LAZY_VALUE.equalsIgnoreCase(loading)) {
                return LAZY;
            } else {
                throw new MapperParsingException("Unknown [" + KEY + "] value: [" + loading + "]");
            }
        }

    }

    Names names();

    FieldType fieldType();

    float boost();

    /**
     * The analyzer that will be used to index the field.
     */
    Analyzer indexAnalyzer();

    /**
     * The analyzer that will be used to search the field.
     */
    Analyzer searchAnalyzer();

    /**
     * The analyzer that will be used for quoted search on the field.
     */
    Analyzer searchQuoteAnalyzer();

    /**
     * Similarity used for scoring queries on the field
     */
    SimilarityProvider similarity();

    /**
     * List of fields where this field should be copied to
     */
    AbstractFieldMapper.CopyTo copyTo();

    /**
     * Returns the actual value of the field.
     */
    Object value(Object value);

    /**
     * Returns the value that will be used as a result for search. Can be only of specific types... .
     */
    Object valueForSearch(Object value);

    /**
     * Returns the indexed value used to construct search "values".
     */
    BytesRef indexedValueForSearch(Object value);

    /**
     * Should the field query {@link #termQuery(Object, org.elasticsearch.index.query.QueryParseContext)}  be used when detecting this
     * field in query string.
     */
    boolean useTermQueryWithQueryString();

    Query termQuery(Object value, @Nullable QueryParseContext context);

    Query termsQuery(List values, @Nullable QueryParseContext context);

    Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions);

    Query prefixQuery(Object value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context);

    Query regexpQuery(Object value, int flags, int maxDeterminizedStates, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context);

    /**
     * A term query to use when parsing a query string. Can return <tt>null</tt>.
     */
    @Nullable
    Query queryStringTermQuery(Term term);

    /**
     * Null value filter, returns <tt>null</tt> if there is no null value associated with the field.
     */
    @Nullable
    Query nullValueFilter();

    FieldDataType fieldDataType();

    boolean isNumeric();

    boolean isSortable();

    boolean supportsNullValue();

    boolean hasDocValues();

    Loading normsLoading(Loading defaultLoading);

    /**
     * Fields might not be available before indexing, for example _all, token_count,...
     * When get is called and these fields are requested, this case needs special treatment.
     *
     * @return If the field is available before indexing or not.
     * */
    boolean isGenerated();

    /**
     * Parse using the provided {@link ParseContext} and return a mapping
     * update if dynamic mappings modified the mappings, or {@code null} if
     * mappings were not modified.
     */
    Mapper parse(ParseContext context) throws IOException;

    /**
     * @return a {@link FieldStats} instance that maps to the type of this field based on the provided {@link Terms} instance.
     */
    FieldStats stats(Terms terms, int maxDoc) throws IOException;

}
