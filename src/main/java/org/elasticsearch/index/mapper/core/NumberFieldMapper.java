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

package org.elasticsearch.index.mapper.core;

import com.carrotsearch.hppc.DoubleOpenHashSet;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongOpenHashSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeResult;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ParseContext;
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
        
        public static final int PRECISION_STEP_8_BIT  = Integer.MAX_VALUE; // 1tpv: 256 terms at most, not useful
        public static final int PRECISION_STEP_16_BIT = 8;                 // 2tpv
        public static final int PRECISION_STEP_32_BIT = 8;                 // 4tpv
        public static final int PRECISION_STEP_64_BIT = 16;                // 4tpv

        public static final FieldType FIELD_TYPE = new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE);

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.freeze();
        }

        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Boolean> COERCE = new Explicit<>(true, false);
    }

    public abstract static class Builder<T extends Builder, Y extends NumberFieldMapper> extends AbstractFieldMapper.Builder<T, Y> {

        private Boolean ignoreMalformed;

        private Boolean coerce;
        
        public Builder(String name, FieldType fieldType, int defaultPrecisionStep) {
            super(name, fieldType);
            fieldType.setNumericPrecisionStep(defaultPrecisionStep);
        }

        public T precisionStep(int precisionStep) {
            fieldType.setNumericPrecisionStep(precisionStep);
            return builder;
        }

        public T ignoreMalformed(boolean ignoreMalformed) {
            this.ignoreMalformed = ignoreMalformed;
            return builder;
        }

        protected Explicit<Boolean> ignoreMalformed(BuilderContext context) {
            if (ignoreMalformed != null) {
                return new Explicit<>(ignoreMalformed, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(context.indexSettings().getAsBoolean("index.mapping.ignore_malformed", Defaults.IGNORE_MALFORMED.value()), false);
            }
            return Defaults.IGNORE_MALFORMED;
        }
        
        public T coerce(boolean coerce) {
            this.coerce = coerce;
            return builder;
        }

        protected Explicit<Boolean> coerce(BuilderContext context) {
            if (coerce != null) {
                return new Explicit<>(coerce, true);
            }
            if (context.indexSettings() != null) {
                return new Explicit<>(context.indexSettings().getAsBoolean("index.mapping.coerce", Defaults.COERCE.value()), false);
            }
            return Defaults.COERCE;
        }
        
    }

    protected int precisionStep;

    protected Boolean includeInAll;

    protected Explicit<Boolean> ignoreMalformed;

    protected Explicit<Boolean> coerce;
    
    /** 
     * True if index version is 1.4+
     * <p>
     * In this case numerics are encoded with SORTED_NUMERIC docvalues,
     * otherwise for older indexes we must continue to write BINARY (for now)
     */
    protected final boolean useSortedNumericDocValues;
    
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
    
    private static ThreadLocal<NumericTokenStream> tokenStream16 = new ThreadLocal<NumericTokenStream>() {
        @Override
        protected NumericTokenStream initialValue() {
            return new NumericTokenStream(16);
        }
    };

    private static ThreadLocal<NumericTokenStream> tokenStreamMax = new ThreadLocal<NumericTokenStream>() {
        @Override
        protected NumericTokenStream initialValue() {
            return new NumericTokenStream(Integer.MAX_VALUE);
        }
    };

    protected NumberFieldMapper(Names names, int precisionStep, float boost, FieldType fieldType, Boolean docValues,
                                Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce, NamedAnalyzer indexAnalyzer,
                                NamedAnalyzer searchAnalyzer, SimilarityProvider similarity,
                                Loading normsLoading, @Nullable Settings fieldDataSettings, Settings indexSettings,
                                MultiFields multiFields, CopyTo copyTo) {
        // LUCENE 4 UPGRADE: Since we can't do anything before the super call, we have to push the boost check down to subclasses
        super(names, boost, fieldType, docValues, indexAnalyzer, searchAnalyzer,
                similarity, normsLoading, fieldDataSettings, indexSettings, multiFields, copyTo);
        if (precisionStep <= 0 || precisionStep >= maxPrecisionStep()) {
            this.precisionStep = Integer.MAX_VALUE;
        } else {
            this.precisionStep = precisionStep;
        }
        this.ignoreMalformed = ignoreMalformed;
        this.coerce = coerce;
        Version v = Version.indexCreated(indexSettings);
        this.useSortedNumericDocValues = v.onOrAfter(Version.V_1_4_0_Beta1);
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

    @Override
    public void unsetIncludeInAll() {
        includeInAll = null;
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
        } catch (IllegalArgumentException | ElasticsearchIllegalArgumentException e1) {
            e = e1;
        } catch (MapperParsingException e2) {
            e = e2;
        }

        if (e != null && !ignoreMalformed.value()) {
            throw e;
        }
    }

    protected abstract void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException;

    protected final void addDocValue(ParseContext context, List<Field> fields, long value) {
        if (useSortedNumericDocValues) {
            fields.add(new SortedNumericDocValuesField(names().indexName(), value));
        } else {
            CustomLongNumericDocValuesField field = (CustomLongNumericDocValuesField) context.doc().getByKey(names().indexName());
            if (field != null) {
                field.add(value);
            } else {
                field = new CustomLongNumericDocValuesField(names().indexName(), value);
                context.doc().addWithKey(names().indexName(), field);
            }
        }
    }

    /**
     * Use the field query created here when matching on numbers.
     */
    @Override
    public boolean useTermQueryWithQueryString() {
        return true;
    }

    @Override
    public final Query termQuery(Object value, @Nullable QueryParseContext context) {
        return new TermQuery(new Term(names.indexName(), indexedValueForSearch(value)));
    }

    @Override
    public final Filter termFilter(Object value, @Nullable QueryParseContext context) {
        // Made this method final because previously many subclasses duplicated
        // the same code, returning a NumericRangeFilter, which should be less
        // efficient than super's default impl of a single TermFilter.
        return super.termFilter(value, context);
    }

    @Override
    public abstract Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    @Override
    public abstract Filter rangeFilter(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    @Override
    public abstract Query fuzzyQuery(String value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions);

    /**
     * A range filter based on the field data cache.
     */
    public abstract Filter rangeFilter(QueryParseContext parseContext, Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, @Nullable QueryParseContext context);

    /**
     * A terms filter based on the field data cache for numeric fields.
     */
    @Override
    public Filter fieldDataTermsFilter(List values, @Nullable QueryParseContext context) {
        IndexNumericFieldData fieldData = context.getForField(this);
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
    public void merge(Mapper mergeWith, MergeResult mergeResult) throws MergeMappingException {
        super.merge(mergeWith, mergeResult);
        if (!this.getClass().equals(mergeWith.getClass())) {
            return;
        }
        if (!mergeResult.simulate()) {
            NumberFieldMapper nfmMergeWith = (NumberFieldMapper) mergeWith;
            this.precisionStep = nfmMergeWith.precisionStep;
            this.includeInAll = nfmMergeWith.includeInAll;
            if (nfmMergeWith.ignoreMalformed.explicit()) {
                this.ignoreMalformed = nfmMergeWith.ignoreMalformed;
            }
            if (nfmMergeWith.coerce.explicit()) {
                this.coerce = nfmMergeWith.coerce;
            }
        }
    }

    @Override
    public void close() {
    }

    protected NumericTokenStream popCachedStream() {
        if (precisionStep == 4) {
            return tokenStream4.get();
        } else if (precisionStep == 8) {
            return tokenStream8.get();
        } else if (precisionStep == 16) {
            return tokenStream16.get();
        } else if (precisionStep == Integer.MAX_VALUE) {
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

    public static abstract class CustomNumericDocValuesField implements IndexableField {

        public static final FieldType TYPE = new FieldType();
        static {
          TYPE.setDocValuesType(DocValuesType.BINARY);
          TYPE.freeze();
        }

        private final String name;

        public CustomNumericDocValuesField(String  name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public IndexableFieldType fieldType() {
            return TYPE;
        }

        @Override
        public float boost() {
            return 1f;
        }

        @Override
        public String stringValue() {
            return null;
        }

        @Override
        public Reader readerValue() {
            return null;
        }

        @Override
        public Number numericValue() {
            return null;
        }

        @Override
        public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) throws IOException {
            return null;
        }

    }

    public static class CustomLongNumericDocValuesField extends CustomNumericDocValuesField {

        public static final FieldType TYPE = new FieldType();
        static {
          TYPE.setDocValuesType(DocValuesType.BINARY);
          TYPE.freeze();
        }

        private final LongArrayList values;

        public CustomLongNumericDocValuesField(String  name, long value) {
            super(name);
            values = new LongArrayList();
            add(value);
        }

        public void add(long value) {
            values.add(value);
        }

        @Override
        public BytesRef binaryValue() {
            CollectionUtils.sortAndDedup(values);

            // here is the trick:
            //  - the first value is zig-zag encoded so that eg. -5 would become positive and would be better compressed by vLong
            //  - for other values, we only encode deltas using vLong
            final byte[] bytes = new byte[values.size() * ByteUtils.MAX_BYTES_VLONG];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
            ByteUtils.writeVLong(out, ByteUtils.zigZagEncode(values.get(0)));
            for (int i = 1; i < values.size(); ++i) {
                final long delta = values.get(i) - values.get(i - 1);
                ByteUtils.writeVLong(out, delta);
            }
            return new BytesRef(bytes, 0, out.getPosition());
        }

    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || ignoreMalformed.explicit()) {
            builder.field("ignore_malformed", ignoreMalformed.value());
        }
        if (includeDefaults || coerce.explicit()) {
            builder.field("coerce", coerce.value());
        }
    }

    @Override
    public boolean isNumeric() {
        return true;
    }
}
