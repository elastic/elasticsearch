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

import java.io.IOException;
import java.io.Reader;
import java.util.List;

import org.apache.lucene.analysis.LegacyNumericTokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.TermBasedFieldType;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

/**
 *
 */
public abstract class LegacyNumberFieldMapper extends FieldMapper implements AllFieldMapper.IncludeInAll {
    // this is private since it has a different default
    private static final Setting<Boolean> COERCE_SETTING =
        Setting.boolSetting("index.mapping.coerce", true, Property.IndexScope);

    public static class Defaults {

        public static final int PRECISION_STEP_8_BIT  = Integer.MAX_VALUE; // 1tpv: 256 terms at most, not useful
        public static final int PRECISION_STEP_16_BIT = 8;                 // 2tpv
        public static final int PRECISION_STEP_32_BIT = 8;                 // 4tpv
        public static final int PRECISION_STEP_64_BIT = 16;                // 4tpv

        public static final Explicit<Boolean> IGNORE_MALFORMED = new Explicit<>(false, false);
        public static final Explicit<Boolean> COERCE = new Explicit<>(true, false);
    }

    public abstract static class Builder<T extends Builder, Y extends LegacyNumberFieldMapper> extends FieldMapper.Builder<T, Y> {

        private Boolean ignoreMalformed;

        private Boolean coerce;

        public Builder(String name, MappedFieldType fieldType, int defaultPrecisionStep) {
            super(name, fieldType, fieldType);
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
                return new Explicit<>(IGNORE_MALFORMED_SETTING.get(context.indexSettings()), false);
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
                return new Explicit<>(COERCE_SETTING.get(context.indexSettings()), false);
            }
            return Defaults.COERCE;
        }

        protected void setupFieldType(BuilderContext context) {
            super.setupFieldType(context);
            int precisionStep = fieldType.numericPrecisionStep();
            if (precisionStep <= 0 || precisionStep >= maxPrecisionStep()) {
                fieldType.setNumericPrecisionStep(Integer.MAX_VALUE);
            }
        }

        protected abstract int maxPrecisionStep();
    }

    public static abstract class NumberFieldType extends TermBasedFieldType {

        public NumberFieldType(LegacyNumericType numericType) {
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
                conflicts.add("mapper [" + name() + "] has different [precision_step] values");
            }
        }

        public abstract NumberFieldType clone();

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, DateTimeZone timeZone) {
            if (timeZone != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones");
            }
            if (format == null) {
                return DocValueFormat.RAW;
            } else {
                return new DocValueFormat.Decimal(format);
            }
        }
    }

    protected Boolean includeInAll;

    protected Explicit<Boolean> ignoreMalformed;

    protected Explicit<Boolean> coerce;

    protected LegacyNumberFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce, Settings indexSettings,
                                MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert fieldType.tokenized() == false;
        this.ignoreMalformed = ignoreMalformed;
        this.coerce = coerce;
    }

    @Override
    protected LegacyNumberFieldMapper clone() {
        return (LegacyNumberFieldMapper) super.clone();
    }

    @Override
    public Mapper includeInAll(Boolean includeInAll) {
        if (includeInAll != null) {
            LegacyNumberFieldMapper clone = clone();
            clone.includeInAll = includeInAll;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    public Mapper includeInAllIfNotSet(Boolean includeInAll) {
        if (includeInAll != null && this.includeInAll == null) {
            LegacyNumberFieldMapper clone = clone();
            clone.includeInAll = includeInAll;
            return clone;
        } else {
            return this;
        }
    }

    @Override
    public Mapper unsetIncludeInAll() {
        if (includeInAll != null) {
            LegacyNumberFieldMapper clone = clone();
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
        fields.add(new SortedNumericDocValuesField(fieldType().name(), value));
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
        LegacyNumberFieldMapper nfmMergeWith = (LegacyNumberFieldMapper) mergeWith;

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

        private ThreadLocal<LegacyNumericTokenStream> tokenStream = new ThreadLocal<LegacyNumericTokenStream>() {
            @Override
            protected LegacyNumericTokenStream initialValue() {
                return new LegacyNumericTokenStream(fieldType().numericPrecisionStep());
            }
        };

        private static ThreadLocal<LegacyNumericTokenStream> tokenStream4 = new ThreadLocal<LegacyNumericTokenStream>() {
            @Override
            protected LegacyNumericTokenStream initialValue() {
                return new LegacyNumericTokenStream(4);
            }
        };

        private static ThreadLocal<LegacyNumericTokenStream> tokenStream8 = new ThreadLocal<LegacyNumericTokenStream>() {
            @Override
            protected LegacyNumericTokenStream initialValue() {
                return new LegacyNumericTokenStream(8);
            }
        };

        private static ThreadLocal<LegacyNumericTokenStream> tokenStream16 = new ThreadLocal<LegacyNumericTokenStream>() {
            @Override
            protected LegacyNumericTokenStream initialValue() {
                return new LegacyNumericTokenStream(16);
            }
        };

        private static ThreadLocal<LegacyNumericTokenStream> tokenStreamMax = new ThreadLocal<LegacyNumericTokenStream>() {
            @Override
            protected LegacyNumericTokenStream initialValue() {
                return new LegacyNumericTokenStream(Integer.MAX_VALUE);
            }
        };

        public CustomNumericField(Number value, MappedFieldType fieldType) {
            super(fieldType.name(), fieldType);
            if (value != null) {
                this.fieldsData = value;
            }
        }

        protected LegacyNumericTokenStream getCachedStream() {
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
