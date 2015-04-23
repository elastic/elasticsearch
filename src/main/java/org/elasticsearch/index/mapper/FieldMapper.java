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
import org.apache.lucene.search.Filter;
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
public interface FieldMapper<T> extends Mapper {

    public static final String DOC_VALUES_FORMAT = "doc_values_format";

    public static class Names {

        private final String name;

        private final String indexName;

        private final String indexNameClean;

        private final String fullName;

        private final String sourcePath;

        public Names(String name) {
            this(name, name, name, name);
        }

        public Names(String name, String indexName, String indexNameClean, String fullName) {
            this(name, indexName, indexNameClean, fullName, fullName);
        }

        public Names(String name, String indexName, String indexNameClean, String fullName, @Nullable String sourcePath) {
            this.name = name;
            this.indexName = indexName;
            this.indexNameClean = indexNameClean;
            this.fullName = fullName;
            this.sourcePath = sourcePath == null ? this.fullName : sourcePath;
        }

        /**
         * The logical name of the field.
         */
        public String name() {
            return name;
        }

        /**
         * The indexed name of the field. This is the name under which we will
         * store it in the index.
         */
        public String indexName() {
            return indexName;
        }

        /**
         * The cleaned index name, before any "path" modifications performed on it.
         */
        public String indexNameClean() {
            return indexNameClean;
        }

        /**
         * The full name, including dot path.
         */
        public String fullName() {
            return fullName;
        }

        /**
         * The dot path notation to extract the value from source.
         */
        public String sourcePath() {
            return sourcePath;
        }

        /**
         * Creates a new index term based on the provided value.
         */
        public Term createIndexNameTerm(String value) {
            return new Term(indexName, value);
        }

        /**
         * Creates a new index term based on the provided value.
         */
        public Term createIndexNameTerm(BytesRef value) {
            return new Term(indexName, value);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;

            Names names = (Names) o;

            if (!fullName.equals(names.fullName)) return false;
            if (!indexName.equals(names.indexName)) return false;
            if (!indexNameClean.equals(names.indexNameClean)) return false;
            if (!name.equals(names.name)) return false;
            if (!sourcePath.equals(names.sourcePath)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + indexName.hashCode();
            result = 31 * result + indexNameClean.hashCode();
            result = 31 * result + fullName.hashCode();
            result = 31 * result + sourcePath.hashCode();
            return result;
        }
    }

    public static enum Loading {
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
    public AbstractFieldMapper.CopyTo copyTo();

    /**
     * Returns the actual value of the field.
     */
    T value(Object value);

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

    Filter termFilter(Object value, @Nullable QueryParseContext context);

    Filter termsFilter(List values, @Nullable QueryParseContext context);

    Filter fieldDataTermsFilter(List values, @Nullable QueryParseContext context);

    Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions);

    Query prefixQuery(Object value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context);

    Filter prefixFilter(Object value, @Nullable QueryParseContext context);

    Query regexpQuery(Object value, int flags, int maxDeterminizedStates, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context);

    Filter regexpFilter(Object value, int flags, int maxDeterminizedStates, @Nullable QueryParseContext parseContext);

    /**
     * A term query to use when parsing a query string. Can return <tt>null</tt>.
     */
    @Nullable
    Query queryStringTermQuery(Term term);

    /**
     * Null value filter, returns <tt>null</tt> if there is no null value associated with the field.
     */
    @Nullable
    Filter nullValueFilter();

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
    public boolean isGenerated();

    /**
     * @return a {@link FieldStats} instance that maps to the type of this field based on the provided {@link Terms} instance.
     */
    FieldStats stats(Terms terms, int maxDoc) throws IOException;

}
