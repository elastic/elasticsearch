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

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * This defines the core properties and functions to operate on a field.
 */
public abstract class MappedFieldType extends FieldType {

    public static class Names {

        private final String indexName;

        private final String originalIndexName;

        private final String fullName;

        public Names(String name) {
            this(name, name, name);
        }

        public Names(String indexName, String originalIndexName, String fullName) {
            this.indexName = indexName;
            this.originalIndexName = originalIndexName;
            this.fullName = fullName;
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

            return true;
        }

        @Override
        public int hashCode() {
            int result = indexName.hashCode();
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
    private Object nullValue;
    private String nullValueAsString; // for sending null value to _all field

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
        this.nullValue = ref.nullValue();
        this.nullValueAsString = ref.nullValueAsString();
    }

    public MappedFieldType() {
        setTokenized(true);
        setStored(false);
        setStoreTermVectors(false);
        setOmitNorms(false);
        setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        setBoost(1.0f);
        fieldDataType = new FieldDataType(typeName());
    }

    @Override
    public abstract MappedFieldType clone();

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) return false;
        MappedFieldType fieldType = (MappedFieldType) o;
        // check similarity first because we need to check the name, and it might be null
        // TODO: SimilarityProvider should have equals?
        if (similarity == null || fieldType.similarity == null) {
            if (similarity != fieldType.similarity) {
                return false;
            }
        } else {
            if (Objects.equals(similarity.name(), fieldType.similarity.name()) == false) {
                return false;
            }
        }

        return boost == fieldType.boost &&
            docValues == fieldType.docValues &&
            Objects.equals(names, fieldType.names) &&
            Objects.equals(indexAnalyzer, fieldType.indexAnalyzer) &&
            Objects.equals(searchAnalyzer, fieldType.searchAnalyzer) &&
            Objects.equals(searchQuoteAnalyzer(), fieldType.searchQuoteAnalyzer()) &&
            Objects.equals(normsLoading, fieldType.normsLoading) &&
            Objects.equals(fieldDataType, fieldType.fieldDataType) &&
            Objects.equals(nullValue, fieldType.nullValue) &&
            Objects.equals(nullValueAsString, fieldType.nullValueAsString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), names, boost, docValues, indexAnalyzer, searchAnalyzer, searchQuoteAnalyzer,
            similarity == null ? null : similarity.name(), normsLoading, fieldDataType, nullValue, nullValueAsString);
    }

    // norelease: we need to override freeze() and add safety checks that all settings are actually set

    /** Returns the name of this type, as would be specified in mapping properties */
    public abstract String typeName();

    /** Checks this type is the same type as other. Adds a conflict if they are different. */
    private final void checkTypeName(MappedFieldType other) {
        if (typeName().equals(other.typeName()) == false) {
            throw new IllegalArgumentException("mapper [" + names().fullName() + "] cannot be changed from type [" + typeName() + "] to [" + other.typeName() + "]");
        } else if (getClass() != other.getClass()) {
            throw new IllegalStateException("Type names equal for class " + getClass().getSimpleName() + " and " + other.getClass().getSimpleName());
        }
    }

    /**
     * Checks for any conflicts between this field type and other.
     * If strict is true, all properties must be equal.
     * Otherwise, only properties which must never change in an index are checked.
     */
    public void checkCompatibility(MappedFieldType other, List<String> conflicts, boolean strict) {
        checkTypeName(other);

        boolean indexed =  indexOptions() != IndexOptions.NONE;
        boolean mergeWithIndexed = other.indexOptions() != IndexOptions.NONE;
        // TODO: should be validating if index options go "up" (but "down" is ok)
        if (indexed != mergeWithIndexed || tokenized() != other.tokenized()) {
            conflicts.add("mapper [" + names().fullName() + "] has different [index] values");
        }
        if (stored() != other.stored()) {
            conflicts.add("mapper [" + names().fullName() + "] has different [store] values");
        }
        if (hasDocValues() == false && other.hasDocValues()) {
            // don't add conflict if this mapper has doc values while the mapper to merge doesn't since doc values are implicitly set
            // when the doc_values field data format is configured
            conflicts.add("mapper [" + names().fullName() + "] has different [doc_values] values, cannot change from disabled to enabled");
        }
        if (omitNorms() && !other.omitNorms()) {
            conflicts.add("mapper [" + names().fullName() + "] has different [omit_norms] values, cannot change from disable to enabled");
        }
        if (storeTermVectors() != other.storeTermVectors()) {
            conflicts.add("mapper [" + names().fullName() + "] has different [store_term_vector] values");
        }
        if (storeTermVectorOffsets() != other.storeTermVectorOffsets()) {
            conflicts.add("mapper [" + names().fullName() + "] has different [store_term_vector_offsets] values");
        }
        if (storeTermVectorPositions() != other.storeTermVectorPositions()) {
            conflicts.add("mapper [" + names().fullName() + "] has different [store_term_vector_positions] values");
        }
        if (storeTermVectorPayloads() != other.storeTermVectorPayloads()) {
            conflicts.add("mapper [" + names().fullName() + "] has different [store_term_vector_payloads] values");
        }

        // null and "default"-named index analyzers both mean the default is used
        if (indexAnalyzer() == null || "default".equals(indexAnalyzer().name())) {
            if (other.indexAnalyzer() != null && "default".equals(other.indexAnalyzer().name()) == false) {
                conflicts.add("mapper [" + names().fullName() + "] has different [analyzer]");
            }
        } else if (other.indexAnalyzer() == null || "default".equals(other.indexAnalyzer().name())) {
            conflicts.add("mapper [" + names().fullName() + "] has different [analyzer]");
        } else if (indexAnalyzer().name().equals(other.indexAnalyzer().name()) == false) {
            conflicts.add("mapper [" + names().fullName() + "] has different [analyzer]");
        }

        if (!names().indexName().equals(other.names().indexName())) {
            conflicts.add("mapper [" + names().fullName() + "] has different [index_name]");
        }
        if (Objects.equals(similarity(), other.similarity()) == false) {
            conflicts.add("mapper [" + names().fullName() + "] has different [similarity]");
        }

        if (strict) {
            if (omitNorms() != other.omitNorms()) {
                conflicts.add("mapper [" + names().fullName() + "] is used by multiple types. Set update_all_types to true to update [omit_norms] across all types.");
            }
            if (boost() != other.boost()) {
                conflicts.add("mapper [" + names().fullName() + "] is used by multiple types. Set update_all_types to true to update [boost] across all types.");
            }
            if (normsLoading() != other.normsLoading()) {
                conflicts.add("mapper [" + names().fullName() + "] is used by multiple types. Set update_all_types to true to update [norms.loading] across all types.");
            }
            if (Objects.equals(searchAnalyzer(), other.searchAnalyzer()) == false) {
                conflicts.add("mapper [" + names().fullName() + "] is used by multiple types. Set update_all_types to true to update [search_analyzer] across all types.");
            }
            if (Objects.equals(searchQuoteAnalyzer(), other.searchQuoteAnalyzer()) == false) {
                conflicts.add("mapper [" + names().fullName() + "] is used by multiple types. Set update_all_types to true to update [search_quote_analyzer] across all types.");
            }
            if (Objects.equals(fieldDataType(), other.fieldDataType()) == false) {
                conflicts.add("mapper [" + names().fullName() + "] is used by multiple types. Set update_all_types to true to update [fielddata] across all types.");
            }
            if (Objects.equals(nullValue(), other.nullValue()) == false) {
                conflicts.add("mapper [" + names().fullName() + "] is used by multiple types. Set update_all_types to true to update [null_value] across all types.");
            }
        }
    }

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

    /** Returns the value that should be added when JSON null is found, or null if no value should be added */
    public Object nullValue() {
        return nullValue;
    }

    /** Returns the null value stringified, so it can be used for e.g. _all field, or null if there is no null value */
    public String nullValueAsString() {
        return nullValueAsString;
    }

    /** Sets the null value and initializes the string version */
    public void setNullValue(Object nullValue) {
        checkIfFrozen();
        this.nullValue = nullValue;
        this.nullValueAsString = nullValue == null ? null : nullValue.toString();
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
     * Should the field query {@link #termQuery(Object, org.elasticsearch.index.query.QueryShardContext)}  be used when detecting this
     * field in query string.
     */
    public boolean useTermQueryWithQueryString() {
        return false;
    }

    /** Creates a term associated with the field of this mapper for the given value */
    protected Term createTerm(Object value) {
        return new Term(names().indexName(), indexedValueForSearch(value));
    }

    public Query termQuery(Object value, @Nullable QueryShardContext context) {
        return new TermQuery(createTerm(value));
    }

    public Query termsQuery(List values, @Nullable QueryShardContext context) {
        BytesRef[] bytesRefs = new BytesRef[values.size()];
        for (int i = 0; i < bytesRefs.length; i++) {
            bytesRefs[i] = indexedValueForSearch(values.get(i));
        }
        return new TermsQuery(names.indexName(), bytesRefs);
    }

    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper) {
        return new TermRangeQuery(names().indexName(),
            lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
            upperTerm == null ? null : indexedValueForSearch(upperTerm),
            includeLower, includeUpper);
    }

    public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions) {
        return new FuzzyQuery(createTerm(value), fuzziness.asDistance(BytesRefs.toString(value)), prefixLength, maxExpansions, transpositions);
    }

    public Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryShardContext context) {
        PrefixQuery query = new PrefixQuery(createTerm(value));
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    public Query regexpQuery(String value, int flags, int maxDeterminizedStates, @Nullable MultiTermQuery.RewriteMethod method, @Nullable QueryShardContext context) {
        if (numericType() != null) {
            throw new QueryShardException(context, "Cannot use regular expression to filter numeric field [" + names.fullName + "]");
        }

        RegexpQuery query = new RegexpQuery(createTerm(value), flags, maxDeterminizedStates);
        if (method != null) {
            query.setRewriteMethod(method);
        }
        return query;
    }

    public Query nullValueQuery() {
        if (nullValue == null) {
            return null;
        }
        return new ConstantScoreQuery(termQuery(nullValue, null));
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
