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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
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
import org.elasticsearch.index.codec.docvaluesformat.DocValuesFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.query.QueryParseContext;
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
                                NamedAnalyzer searchAnalyzer, PostingsFormatProvider postingsProvider,
                                DocValuesFormatProvider docValuesProvider, SimilarityProvider similarity,
                                @Nullable Settings fieldDataSettings, Settings indexSettings) {
        // LUCENE 4 UPGRADE: Since we can't do anything before the super call, we have to push the boost check down to subclasses
        super(names, boost, fieldType, indexAnalyzer, searchAnalyzer, postingsProvider, docValuesProvider, similarity, fieldDataSettings, indexSettings);
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
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        RuntimeException e = null;
        try {
            innerParseCreateField(context, fields);
        } catch (IllegalArgumentException e1) {
            e = e1;
        } catch (MapperParsingException e2) {
            e = e2;
        }

        if (e != null && !ignoreMalformed.value()) {
            throw e;
        }
    }

    /**
     * Utility method to convert a long to a doc values field using {@link NumericUtils} encoding.
     */
    protected final Field toDocValues(long l) {
        final BytesRef bytes = new BytesRef();
        NumericUtils.longToPrefixCoded(l, 0, bytes);
        return new SortedSetDocValuesField(names().indexName(), bytes);
    }

    /**
     * Utility method to convert an int to a doc values field using {@link NumericUtils} encoding.
     */
    protected final Field toDocValues(int i) {
        final BytesRef bytes = new BytesRef();
        NumericUtils.intToPrefixCoded(i, 0, bytes);
        return new SortedSetDocValuesField(names().indexName(), bytes);
    }

    /**
     * Utility method to convert a float to a doc values field using {@link NumericUtils} encoding.
     */
    protected final Field toDocValues(float f) {
        return toDocValues(NumericUtils.floatToSortableInt(f));
    }

    /**
     * Utility method to convert a double to a doc values field using {@link NumericUtils} encoding.
     */
    protected final Field toDocValues(double d) {
        return toDocValues(NumericUtils.doubleToSortableLong(d));
    }

    protected abstract void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException;

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
