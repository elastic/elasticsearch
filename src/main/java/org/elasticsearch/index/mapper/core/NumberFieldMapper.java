/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.core;

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.document.AbstractField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;

import java.io.Reader;

/**
 *
 */
public abstract class NumberFieldMapper<T extends Number> extends AbstractFieldMapper<T> implements AllFieldMapper.IncludeInAll {

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final int PRECISION_STEP = NumericUtils.PRECISION_STEP_DEFAULT;
        public static final Field.Index INDEX = Field.Index.NOT_ANALYZED;
        public static final boolean OMIT_NORMS = true;
        public static final boolean OMIT_TERM_FREQ_AND_POSITIONS = true;
        public static final String FUZZY_FACTOR = null;
    }

    public abstract static class Builder<T extends Builder, Y extends NumberFieldMapper> extends AbstractFieldMapper.Builder<T, Y> {

        protected int precisionStep = Defaults.PRECISION_STEP;

        protected String fuzzyFactor = Defaults.FUZZY_FACTOR;

        public Builder(String name) {
            super(name);
            this.index = Defaults.INDEX;
            this.omitNorms = Defaults.OMIT_NORMS;
            this.omitTermFreqAndPositions = Defaults.OMIT_TERM_FREQ_AND_POSITIONS;
        }

        @Override
        public T store(Field.Store store) {
            return super.store(store);
        }

        @Override
        public T boost(float boost) {
            return super.boost(boost);
        }

        @Override
        public T indexName(String indexName) {
            return super.indexName(indexName);
        }

        @Override
        public T includeInAll(Boolean includeInAll) {
            return super.includeInAll(includeInAll);
        }

        public T precisionStep(int precisionStep) {
            this.precisionStep = precisionStep;
            return builder;
        }

        public T fuzzyFactor(String fuzzyFactor) {
            this.fuzzyFactor = fuzzyFactor;
            return builder;
        }
    }

    protected int precisionStep;

    protected String fuzzyFactor;

    protected double dFuzzyFactor;

    protected Boolean includeInAll;

    private ThreadLocal<NumericTokenStream> tokenStream = new ThreadLocal<NumericTokenStream>() {
        @Override
        protected NumericTokenStream initialValue() {
            return new NumericTokenStream(precisionStep);
        }
    };

    protected NumberFieldMapper(Names names, int precisionStep, @Nullable String fuzzyFactor,
                                Field.Index index, Field.Store store,
                                float boost, boolean omitNorms, boolean omitTermFreqAndPositions,
                                NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer) {
        super(names, index, store, Field.TermVector.NO, boost, boost != 1.0f || omitNorms, omitTermFreqAndPositions, indexAnalyzer, searchAnalyzer);
        if (precisionStep <= 0 || precisionStep >= maxPrecisionStep()) {
            this.precisionStep = Integer.MAX_VALUE;
        } else {
            this.precisionStep = precisionStep;
        }
        this.fuzzyFactor = fuzzyFactor;
        this.dFuzzyFactor = parseFuzzyFactor(fuzzyFactor);
    }

    protected double parseFuzzyFactor(String fuzzyFactor) {
        if (fuzzyFactor == null) {
            return 1.0d;
        }
        return Double.parseDouble(fuzzyFactor);
    }

    @Override
    public void includeInAll(Boolean includeInAll) {
        if (includeInAll != null) {
            this.includeInAll = includeInAll;
        }
    }

    @Override
    public void includeInAllIfNotSet(Boolean includeInAll) {
        if (includeInAll != null && this.includeInAll == null) {
            this.includeInAll = includeInAll;
        }
    }

    protected abstract int maxPrecisionStep();

    public int precisionStep() {
        return this.precisionStep;
    }

    /**
     * Use the field query created here when matching on numbers.
     */
    @Override
    public boolean useFieldQueryWithQueryString() {
        return true;
    }

    /**
     * Numeric field level query are basically range queries with same value and included. That's the recommended
     * way to execute it.
     */
    @Override
    public Query fieldQuery(String value, @Nullable QueryParseContext context) {
        return rangeQuery(value, value, true, true, context);
    }

    @Override
    public abstract Query fuzzyQuery(String value, String minSim, int prefixLength, int maxExpansions);

    @Override
    public abstract Query fuzzyQuery(String value, double minSim, int prefixLength, int maxExpansions);

    /**
     * Numeric field level filter are basically range queries with same value and included. That's the recommended
     * way to execute it.
     */
    @Override
    public Filter fieldFilter(String value, @Nullable QueryParseContext context) {
        return rangeFilter(value, value, true, true, context);
    }

    @Override
    public abstract Query rangeQuery(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    @Override
    public abstract Filter rangeFilter(String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    /**
     * A range filter based on the field data cache.
     */
    public abstract Filter rangeFilter(FieldDataCache fieldDataCache, String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    /**
     * Override the default behavior (to return the string, and return the actual Number instance).
     */
    @Override
    public Object valueForSearch(Fieldable field) {
        return value(field);
    }

    @Override
    public String valueAsString(Fieldable field) {
        Number num = value(field);
        return num == null ? null : num.toString();
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeContext.mergeFlags().simulate()) {
            this.precisionStep = ((NumberFieldMapper) mergeWith).precisionStep;
            this.includeInAll = ((NumberFieldMapper) mergeWith).includeInAll;
            this.fuzzyFactor = ((NumberFieldMapper) mergeWith).fuzzyFactor;
            this.dFuzzyFactor = parseFuzzyFactor(this.fuzzyFactor);
        }
    }

    @Override
    public void close() {
        tokenStream.remove();
    }

    @Override
    public abstract FieldDataType fieldDataType();

    protected NumericTokenStream popCachedStream() {
        return tokenStream.get();
    }

    // used to we can use a numeric field in a document that is then parsed twice!
    public abstract static class CustomNumericField extends AbstractField {

        protected final NumberFieldMapper mapper;

        public CustomNumericField(NumberFieldMapper mapper, byte[] value) {
            this.mapper = mapper;
            this.name = mapper.names().indexName();
            fieldsData = value;

            isIndexed = mapper.indexed();
            isTokenized = mapper.indexed();
            indexOptions = FieldInfo.IndexOptions.DOCS_ONLY;
            omitNorms = mapper.omitNorms();

            if (value != null) {
                isStored = true;
                isBinary = true;
                binaryLength = value.length;
                binaryOffset = 0;
            }

            setStoreTermVector(Field.TermVector.NO);
        }

        @Override
        public String stringValue() {
            return null;
        }

        @Override
        public Reader readerValue() {
            return null;
        }

        public abstract String numericAsString();
    }
}
