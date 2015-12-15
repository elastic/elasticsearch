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
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 *
 */
public abstract class NumberFieldMapper extends FieldMapper implements AllFieldMapper.IncludeInAll {

    public static class Defaults {
        
        public static final int PRECISION_STEP_8_BIT  = Integer.MAX_VALUE; // 1tpv: 256 terms at most, not useful
        public static final int PRECISION_STEP_16_BIT = 8;                 // 2tpv
        public static final int PRECISION_STEP_32_BIT = 8;                 // 4tpv
        public static final int PRECISION_STEP_64_BIT = 16;                // 4tpv

        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Boolean> COERCE = new Explicit<>(true, false);
    }

    public abstract static class Builder<T extends Builder, Y extends NumberFieldMapper> extends FieldMapper.Builder<T, Y> {

        private Boolean ignoreMalformed;

        private Boolean coerce;
        
        public Builder(String name, MappedFieldType fieldType, int defaultPrecisionStep) {
            super(name, fieldType);
            this.fieldType.setNumericPrecisionStep(defaultPrecisionStep);
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

        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            fieldType.setOmitNorms(fieldType.omitNorms() && fieldType.boost() == 1.0f);
            int precisionStep = fieldType.numericPrecisionStep();
            if (precisionStep <= 0 || precisionStep >= maxPrecisionStep()) {
                fieldType.setNumericPrecisionStep(Integer.MAX_VALUE);
            }
            fieldType.setIndexAnalyzer(makeNumberAnalyzer(fieldType.numericPrecisionStep()));
            fieldType.setSearchAnalyzer(makeNumberAnalyzer(Integer.MAX_VALUE));
        }

        protected abstract NamedAnalyzer makeNumberAnalyzer(int precisionStep);

        protected abstract int maxPrecisionStep();
    }

    public static abstract class NumberFieldType extends MappedFieldType {

        public NumberFieldType(NumericType numericType) {
            setTokenized(false);
            setOmitNorms(true);
            setIndexOptions(IndexOptions.DOCS);
            setStoreTermVectors(false);
            setNumericType(numericType);
        }

        protected NumberFieldType(NumberFieldType ref) {
            super(ref);
        }

        @Override
        public void checkCompatibility(MappedFieldType other,
                List<String> conflicts, boolean strict) {
            super.checkCompatibility(other, conflicts, strict);
            if (numericPrecisionStep() != other.numericPrecisionStep()) {
                conflicts.add("mapper [" + names().fullName() + "] has different [precision_step] values");
            }
        }

        public abstract NumberFieldType clone();

        @Override
        public abstract Object value(Object value);

        @Override
        public Object valueForSearch(Object value) {
            return value(value);
        }

        @Override
        public abstract Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions, boolean transpositions);

        @Override
        public boolean useTermQueryWithQueryString() {
            return true;
        }

        @Override
        public boolean isNumeric() {
            return true;
        }
    }

    protected Boolean includeInAll;

    protected Explicit<Boolean> ignoreMalformed;

    protected Explicit<Boolean> coerce;
    
    protected NumberFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce, Settings indexSettings,
                                MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.ignoreMalformed = ignoreMalformed;
        this.coerce = coerce;
    }

    @Override
    protected NumberFieldMapper clone() {
        return (NumberFieldMapper) super.clone();
    }

    @Override
    public Mapper includeInAll(Boolean includeInAll) {
        if (includeInAll != null) {
            NumberFieldMapper clone = clone();
            clone.includeInAll = includeInAll;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    public Mapper includeInAllIfNotSet(Boolean includeInAll) {
        if (includeInAll != null && this.includeInAll == null) {
            NumberFieldMapper clone = clone();
            clone.includeInAll = includeInAll;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    public Mapper unsetIncludeInAll() {
        if (includeInAll != null) {
            NumberFieldMapper clone = clone();
            clone.includeInAll = null;
            return clone;
        } else {
            return this;
        }
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

    protected abstract void innerParseCreateField(ParseContext context, List<Field> fields) throws IOException;

    protected final void addDocValue(ParseContext context, List<Field> fields, long value) {
        fields.add(new SortedNumericDocValuesField(fieldType().names().indexName(), value));
    }

    /**
     * Converts an object value into a double
     */
    public static double parseDoubleValue(Object value) {
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
    public static long parseLongValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        if (value instanceof BytesRef) {
            return Long.parseLong(((BytesRef) value).utf8ToString());
        }

        return Long.parseLong(value.toString());
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        super.doMerge(mergeWith, updateAllTypes);
        NumberFieldMapper nfmMergeWith = (NumberFieldMapper) mergeWith;

        this.includeInAll = nfmMergeWith.includeInAll;
        if (nfmMergeWith.ignoreMalformed.explicit()) {
            this.ignoreMalformed = nfmMergeWith.ignoreMalformed;
        }
        if (nfmMergeWith.coerce.explicit()) {
            this.coerce = nfmMergeWith.coerce;
        }
    }

    // used to we can use a numeric field in a document that is then parsed twice!
    public abstract static class CustomNumericField extends Field {

        private ThreadLocal<NumericTokenStream> tokenStream = new ThreadLocal<NumericTokenStream>() {
            @Override
            protected NumericTokenStream initialValue() {
                return new NumericTokenStream(fieldType().numericPrecisionStep());
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

        public CustomNumericField(Number value, MappedFieldType fieldType) {
            super(fieldType.names().indexName(), fieldType);
            if (value != null) {
                this.fieldsData = value;
            }
        }

        protected NumericTokenStream getCachedStream() {
            if (fieldType().numericPrecisionStep() == 4) {
                return tokenStream4.get();
            } else if (fieldType().numericPrecisionStep() == 8) {
                return tokenStream8.get();
            } else if (fieldType().numericPrecisionStep() == 16) {
                return tokenStream16.get();
            } else if (fieldType().numericPrecisionStep() == Integer.MAX_VALUE) {
                return tokenStreamMax.get();
            }
            return tokenStream.get();
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
}
