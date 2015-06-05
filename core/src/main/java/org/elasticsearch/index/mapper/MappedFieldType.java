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
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.List;

/**
 * This defines the core properties and functions to operate on a field.
 */
public class MappedFieldType extends FieldType {

    public static class Names {

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

    public enum Loading {
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

    private Names names;
    private float boost;
    // TODO: remove this docvalues flag and use docValuesType
    private boolean docValues;
    private NamedAnalyzer indexAnalyzer;
    private NamedAnalyzer searchAnalyzer;
    private NamedAnalyzer searchQuoteAnalyzer;
    private SimilarityProvider similarity;
    private Loading normsLoading;
    private FieldDataType fieldDataType;

    protected MappedFieldType(MappedFieldType ref) {
        super(ref);
        this.names = ref.names();
        this.boost = ref.boost();
        this.docValues = ref.hasDocValues();
        this.indexAnalyzer = ref.indexAnalyzer();
        this.searchAnalyzer = ref.searchAnalyzer();
        this.searchQuoteAnalyzer = ref.searchQuoteAnalyzer();
        this.similarity = ref.similarity();
        this.normsLoading = ref.normsLoading();
        this.fieldDataType = ref.fieldDataType();
    }

    public MappedFieldType() {}

    public MappedFieldType clone() {
        return new MappedFieldType(this);
    }

    // norelease: we need to override freeze() and add safety checks that all settings are actually set

    public boolean isNumeric() {
        return false;
    }

    public boolean isSortable() {
        return true;
    }

    public Names names() {
        return names;
    }

    public void setNames(Names names) {
        checkIfFrozen();
        this.names = names;
    }

    public float boost() {
        return boost;
    }

    public void setBoost(float boost) {
        checkIfFrozen();
        this.boost = boost;
    }

    public FieldDataType fieldDataType() {
        return fieldDataType;
    }

    public void setFieldDataType(FieldDataType fieldDataType) {
        checkIfFrozen();
        this.fieldDataType = fieldDataType;
    }

    public boolean hasDocValues() {
        return docValues;
    }

    public void setHasDocValues(boolean hasDocValues) {
        checkIfFrozen();
        this.docValues = hasDocValues;
    }

    public Loading normsLoading() {
        return normsLoading;
    }

    public void setNormsLoading(Loading normsLoading) {
        checkIfFrozen();
        this.normsLoading = normsLoading;
    }

    public NamedAnalyzer indexAnalyzer() {
        return indexAnalyzer;
    }

    public void setIndexAnalyzer(NamedAnalyzer analyzer) {
        checkIfFrozen();
        this.indexAnalyzer = analyzer;
    }

    public NamedAnalyzer searchAnalyzer() {
        return searchAnalyzer;
    }

    public void setSearchAnalyzer(NamedAnalyzer analyzer) {
        checkIfFrozen();
        this.searchAnalyzer = analyzer;
    }

    public NamedAnalyzer searchQuoteAnalyzer() {
        return searchQuoteAnalyzer == null ? searchAnalyzer : searchQuoteAnalyzer;
    }

    public void setSearchQuoteAnalyzer(NamedAnalyzer analyzer) {
        checkIfFrozen();
        this.searchQuoteAnalyzer = analyzer;
    }

    public SimilarityProvider similarity() {
        return similarity;
    }

    public void setSimilarity(SimilarityProvider similarity) {
        checkIfFrozen();
        this.similarity = similarity;
    }

    /** Returns the actual value of the field. */
    public Object value(Object value) {
        return value;
    }

    /** Returns the value that will be used as a result for search. Can be only of specific types... */
    public Object valueForSearch(Object value) {
        return value;
    }

    /** Returns the indexed value used to construct search "values". */
    public BytesRef indexedValueForSearch(Object value) {
        return BytesRefs.toBytesRef(value);
    }

    /**
     * Should the field query {@link #termQuery(Object, org.elasticsearch.index.query.QueryParseContext)}  be used when detecting this
     * field in query string.
     */
    public boolean useTermQueryWithQueryString() {
        return false;
    }

    /** Creates a term associated with the field of this mapper for the given value */
    protected Term createTerm(Object value) {
        return new Term(names().indexName(), indexedValueForSearch(value));
    }

    public Query termQuery(Object value, @Nullable QueryParseContext context) {
        return new TermQuery(createTerm(value));
    }

    public Query termsQuery(List values, @Nullable QueryParseContext context) {
        BytesRef[] bytesRefs = new BytesRef[values.size()];
        for (int i = 0; i < bytesRefs.length; i++) {
            bytesRefs[i] = indexedValueForSearch(values.get(i));
        }
        return new TermsQuery(names.indexName(), bytesRefs);
    }

    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context) {
        return new TermRangeQuery(names().indexName(),
            lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
            upperTerm == null ? null : indexedValueForSearch(upperTerm),
            includeLower, includeUpper);
    }

    public Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        return new FuzzyQuery(createTerm(value), fuzziness.asDistance(value), prefixLength, maxExpansions, transpositions);
    }

    public Query prefixQuery(Object value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        PrefixQuery query = new PrefixQuery(createTerm(value));
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    public Query regexpQuery(Object value, int flags, int maxDeterminizedStates, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryParseContext context) {
        RegexpQuery query = new RegexpQuery(createTerm(value), flags, maxDeterminizedStates);
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    /**
     * @return a {@link FieldStats} instance that maps to the type of this field based on the provided {@link Terms} instance.
     */
    public FieldStats stats(Terms terms, int maxDoc) throws IOException {
        return new FieldStats.Text(
            maxDoc, terms.getDocCount(), terms.getSumDocFreq(), terms.getSumTotalTermFreq(), terms.getMin(), terms.getMax()
        );
    }

    /** A term query to use when parsing a query string. Can return <tt>null</tt>. */
    @Nullable
    public Query queryStringTermQuery(Term term) {
        return null;
    }
}
