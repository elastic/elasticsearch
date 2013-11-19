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

import com.carrotsearch.hppc.DoubleOpenHashSet;
import com.carrotsearch.hppc.LongOpenHashSet;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.search.FieldDataTermsFilter;
import org.elasticsearch.index.similarity.SimilarityProvider;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 *
 */
public abstract class NumberFieldMapper<T extends Number> extends AbstractFieldMapper<T> implements AllFieldMapper.IncludeInAll {

    public static class Defaults extends AbstractFieldMapper.Defaults {
        public static final int PRECISION_STEP = NumericUtils.PRECISION_STEP_DEFAULT;

        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.freeze();
        }

        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<Boolean>(false, false);
    }

    public abstract static class Builder<T extends Builder, Y extends NumberFieldMapper> extends AbstractFieldMapper.Builder<T, Y> {

        protected int precisionStep = Defaults.PRECISION_STEP;

        private Boolean ignoreMalformed;

        public Builder(String name, FieldType fieldType) {
            super(name, fieldType);
        }

        @Override
        public T store(boolean store) {
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

        public T ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<Boolean>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<Boolean>(context.indexSettings().getAsBoolean("index.mapping.ignore_malformed", Defaults.IGNORE_MALFORMED.value()), false);
            }
            return Defaults.IGNORE_MALFORMED;
        }
    }

    protected int precisionStep;

    protected Boolean includeInAll;

    protected Explicit<Boolean> ignoreMalformed;

    private ThreadLocal<NumericTokenStream> tokenStream = new ThreadLocal<NumericTokenStream>() {
        @Override
        protected NumericTokenStream initialValue() {
            return new NumericTokenStream(precisionStep);
        }
    };

    private static ThreadLocal<NumericTokenStream> tokenStream4 = new ThreadLocal<NumericTokenStream>() {
        @Override
        protected NumericTokenStream initialValue() {
            return new NumericTokenStream(4);
        }
    };

    private static ThreadLocal<NumericTokenStream> tokenStream8 = new ThreadLocal<NumericTokenStream>() {
        @Override
        protected NumericTokenStream initialValue() {
            return new NumericTokenStream(8);
        }
    };

    private static ThreadLocal<NumericTokenStream> tokenStreamMax = new ThreadLocal<NumericTokenStream>() {
        @Override
        protected NumericTokenStream initialValue() {
            return new NumericTokenStream(Integer.MAX_VALUE);
        }
    };

    protected NumberFieldMapper(Names names, int precisionStep, float boost, FieldType fieldType,
                                Explicit<Boolean> ignoreMalformed, NamedAnalyzer indexAnalyzer,
                                NamedAnalyzer searchAnalyzer, PostingsFormatProvider provider, SimilarityProvider similarity,
                                @Nullable Settings fieldDataSettings) {
        // LUCENE 4 UPGRADE: Since we can't do anything before the super call, we have to push the boost check down to subclasses
        super(names, boost, fieldType, indexAnalyzer, searchAnalyzer, provider, similarity, fieldDataSettings);
        if (precisionStep <= 0 || precisionStep >= maxPrecisionStep()) {
            this.precisionStep = Integer.MAX_VALUE;
        } else {
            this.precisionStep = precisionStep;
        }
        this.ignoreMalformed = ignoreMalformed;
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

    @Override
    protected Field parseCreateField(ParseContext context) throws IOException {
        RuntimeException e;
        try {
            return innerParseCreateField(context);
        } catch (IllegalArgumentException e1) {
            e = e1;
        } catch (MapperParsingException e2) {
            e = e2;
        }

        if (ignoreMalformed.value()) {
            return null;
        } else {
            throw e;
        }
    }

    protected abstract Field innerParseCreateField(ParseContext context) throws IOException;

    /**
     * Use the field query created here when matching on numbers.
     */
    @Override
    public boolean useTermQueryWithQueryString() {
        return true;
    }

    /**
     * Numeric field level query are basically range queries with same value and included. That's the recommended
     * way to execute it.
     */
    @Override
    public Query termQuery(Object value, @Nullable QueryParseContext context) {
        return rangeQuery(value, value, true, true, context);
    }

    /**
     * Numeric field level filter are basically range queries with same value and included. That's the recommended
     * way to execute it.
     */
    @Override
    public Filter termFilter(Object value, @Nullable QueryParseContext context) {
        return rangeFilter(value, value, true, true, context);
    }

    @Override
    public abstract Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    @Override
    public abstract Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    @Override
    public abstract Query fuzzyQuery(String value, String minSim, int prefixLength, int maxExpansions, boolean transpositions);

    /**
     * A range filter based on the field data cache.
     */
    public abstract Filter rangeFilter(IndexFieldDataService fieldData, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    /**
     * A terms filter based on the field data cache for numeric fields.
     */
    @Override
    public Filter termsFilter(IndexFieldDataService fieldDataService, List values, @Nullable QueryParseContext context) {
        IndexNumericFieldData fieldData = fieldDataService.getForField(this);
        if (fieldData.getNumericType().isFloatingPoint()) {
            // create with initial size large enough to avoid rehashing
            DoubleOpenHashSet terms =
                    new DoubleOpenHashSet((int) (values.size() * (1 + DoubleOpenHashSet.DEFAULT_LOAD_FACTOR)));
            for (int i = 0, len = values.size(); i < len; i++) {
                terms.add(parseDoubleValue(values.get(i)));
            }

            return FieldDataTermsFilter.newDoubles(fieldData, terms);
        } else {
            // create with initial size large enough to avoid rehashing
            LongOpenHashSet terms =
                    new LongOpenHashSet((int) (values.size() * (1 + LongOpenHashSet.DEFAULT_LOAD_FACTOR)));
            for (int i = 0, len = values.size(); i < len; i++) {
                terms.add(parseLongValue(values.get(i)));
            }

            return FieldDataTermsFilter.newLongs(fieldData, terms);
        }
    }

    /**
     * Converts an object value into a double
     */
    public double parseDoubleValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        if (value instanceof BytesRef) {
            return Double.parseDouble(((BytesRef) value).utf8ToString());
        }

        return Double.parseDouble(value.toString());
    }

    /**
     * Converts an object value into a long
     */
    public long parseLongValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        if (value instanceof BytesRef) {
            return Long.parseLong(((BytesRef) value).utf8ToString());
        }

        return Long.parseLong(value.toString());
    }

    /**
     * Override the default behavior (to return the string, and return the actual Number instance).
     *
     * @param value
     */
    @Override
    public Object valueForSearch(Object value) {
        return value(value);
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        super.merge(mergeWith, mergeContext);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeContext.mergeFlags().simulate()) {
            NumberFieldMapper nfmMergeWith = (NumberFieldMapper) mergeWith;
            this.precisionStep = nfmMergeWith.precisionStep;
            this.includeInAll = nfmMergeWith.includeInAll;
            if (nfmMergeWith.ignoreMalformed.explicit()) {
                this.ignoreMalformed = nfmMergeWith.ignoreMalformed;
            }
        }
    }

    @Override
    public void close() {
    }

    protected NumericTokenStream popCachedStream() {
        if (precisionStep == 4) {
            return tokenStream4.get();
        }
        if (precisionStep == 8) {
            return tokenStream8.get();
        }
        if (precisionStep == Integer.MAX_VALUE) {
            return tokenStreamMax.get();
        }
        return tokenStream.get();
    }

    // used to we can use a numeric field in a document that is then parsed twice!
    public abstract static class CustomNumericField extends Field {

        protected final NumberFieldMapper mapper;

        public CustomNumericField(NumberFieldMapper mapper, Number value, FieldType fieldType) {
            super(mapper.names().indexName(), fieldType);
            this.mapper = mapper;
            if (value != null) {
                this.fieldsData = value;
            }
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

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field("ignore_malformed", ignoreMalformed.value());
        }
    }

    @Override
    public boolean isNumeric() {
        return true;
    }
}
