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

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;
import org.apache.lucene.collation.ICUCollationDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.IndexableBinaryStringTools;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.search.DocValueFormat;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

public class ICUCollationFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "icu_collation";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new CollationFieldType();
    }

    public static final class CollationFieldType extends StringFieldType {
        public CollationFieldType() {
            setStored(false);
            setTokenized(false);
            setIndexOptions(IndexOptions.NONE);
            setHasDocValues(true);
            setDocValuesType(DocValuesType.SORTED);
            freeze();
        }

        protected CollationFieldType(CollationFieldType ref) {
            super(ref);
        }

        public CollationFieldType clone() {
            return new CollationFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder() {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder();
        }

        public static DocValueFormat COLLATE_FORMAT = new DocValueFormat() {
            @Override
            public String getWriteableName() {
                return "collate";
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
            }

            @Override
            public String format(long value) {
                return Long.toString(value);
            }

            @Override
            public String format(double value) {
                return Double.toString(value);
            }

            @Override
            public String format(BytesRef value) {
                int encodedLength = IndexableBinaryStringTools.getEncodedLength(value.bytes, value.offset, value.length);
                char[] encoded = new char[encodedLength];
                IndexableBinaryStringTools.encode(value.bytes, value.offset, value.length, encoded, 0, encodedLength);
                return new String(encoded, 0, encodedLength);
            }

            @Override
            public long parseLong(String value, boolean roundUp, LongSupplier now) {
                double d = Double.parseDouble(value);
                if (roundUp) {
                    d = Math.ceil(d);
                } else {
                    d = Math.floor(d);
                }
                return Math.round(d);
            }

            @Override
            public double parseDouble(String value, boolean roundUp, LongSupplier now) {
                return Double.parseDouble(value);
            }

            @Override
            public BytesRef parseBytesRef(String value) {
                char[] encoded = value.toCharArray();
                int decodedLength = IndexableBinaryStringTools.getDecodedLength(encoded, 0, encoded.length);
                byte[] decoded = new byte[decodedLength];
                IndexableBinaryStringTools.decode(encoded, 0, encoded.length, decoded, 0, decodedLength);
                return new BytesRef(decoded);
            }
        };

        @Override
        public DocValueFormat docValueFormat(final String format, final DateTimeZone timeZone) {
            return COLLATE_FORMAT;
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, ICUCollationFieldMapper> {
        private String rules = null;
        private String language = null;
        private String country = null;
        private String variant = null;
        private String strength = null;
        private String decomposition = null;
        private String alternate = null;
        private boolean caseLevel = false;
        private String caseFirst = null;
        private boolean numeric = false;
        private String variableTop = null;
        private boolean hiraganaQuaternaryMode = false;

        public String rules() {
            return rules;
        }

        public Builder rules(final String rules) {
            this.rules = rules;
            return this;
        }

        public String language() {
            return language;
        }

        public Builder language(final String language) {
            this.language = language;
            return this;
        }

        public String country() {
            return country;
        }

        public Builder country(final String country) {
            this.country = country;
            return this;
        }

        public String variant() {
            return variant;
        }

        public Builder variant(final String variant) {
            this.variant = variant;
            return this;
        }

        public String strength() {
            return strength;
        }

        public Builder strength(final String strength) {
            this.strength = strength;
            return this;
        }

        public String decomposition() {
            return decomposition;
        }

        public Builder decomposition(final String decomposition) {
            this.decomposition = decomposition;
            return this;
        }

        public String alternate() {
            return alternate;
        }

        public Builder alternate(final String alternate) {
            this.alternate = alternate;
            return this;
        }

        public boolean caseLevel() {
            return caseLevel;
        }

        public Builder caseLevel(final boolean caseLevel) {
            this.caseLevel = caseLevel;
            return this;
        }

        public String caseFirst() {
            return caseFirst;
        }

        public Builder caseFirst(final String caseFirst) {
            this.caseFirst = caseFirst;
            return this;
        }

        public boolean numeric() {
            return numeric;
        }

        public Builder numeric(final boolean numeric) {
            this.numeric = numeric;
            return this;
        }

        public String variableTop() {
            return variableTop;
        }

        public Builder variableTop(final String variableTop) {
            this.variableTop = variableTop;
            return this;
        }

        public boolean hiraganaQuaternaryMode() {
            return hiraganaQuaternaryMode;
        }

        public Builder hiraganaQuaternaryMode(final boolean hiraganaQuaternaryMode) {
            this.hiraganaQuaternaryMode = hiraganaQuaternaryMode;
            return this;
        }

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public ICUCollationFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new ICUCollationFieldMapper(name, fieldType, defaultFieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), copyTo, rules, language, country, variant, strength, decomposition,
                alternate, caseLevel, caseFirst, numeric, variableTop, hiraganaQuaternaryMode);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            Builder builder = new Builder(name);

            if (node.get("doc_values") != null) {
                throw new MapperParsingException("Setting [doc_values] cannot be modified for field [" + name + "]");
            }

            if (node.get("index") != null) {
                throw new MapperParsingException("Setting [index] cannot be modified for field [" + name + "]");
            }

            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                switch (fieldName) {
                    case "rules":
                        builder.rules(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "language":
                        builder.language(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "country":
                        builder.country(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "variant":
                        builder.variant(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "strength":
                        builder.strength(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "decomposition":
                        builder.decomposition(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "alternate":
                        builder.alternate(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "caseLevel":
                        builder.caseLevel(XContentMapValues.nodeBooleanValue(fieldNode, false));
                        iterator.remove();
                        break;
                    case "caseFirst":
                        builder.caseFirst(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "numeric":
                        builder.numeric(XContentMapValues.nodeBooleanValue(fieldNode, false));
                        iterator.remove();
                        break;
                    case "variableTop":
                        builder.variableTop(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "hiraganaQuaternaryMode":
                        builder.hiraganaQuaternaryMode(XContentMapValues.nodeBooleanValue(fieldNode, false));
                        iterator.remove();
                        break;
                    default:
                        break;
                }
            }

            TypeParsers.parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    private final String rules;
    private final String language;
    private final String country;
    private final String variant;
    private final String strength;
    private final String decomposition;
    private final String alternate;
    private final boolean caseLevel;
    private final String caseFirst;
    private final boolean numeric;
    private final String variableTop;
    private final boolean hiraganaQuaternaryMode;
    private final Collator collator;

    // each thread needs its own ICUCollationDocValuesField because the internally cloned collator is not thread-safe
    private final ThreadLocal<ICUCollationDocValuesField> perThreadICUCollationField;

    protected ICUCollationFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                      Settings indexSettings, MultiFields multiFields, CopyTo copyTo, String rules, String language,
                                      String country, String variant,
                                      String strength, String decomposition, String alternate, boolean caseLevel, String caseFirst,
                                      boolean numeric, String variableTop, boolean hiraganaQuaternaryMode) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.rules = rules;
        this.language = language;
        this.country = country;
        this.variant = variant;
        this.strength = strength;
        this.decomposition = decomposition;
        this.alternate = alternate;
        this.caseLevel = caseLevel;
        this.caseFirst = caseFirst;
        this.numeric = numeric;
        this.variableTop = variableTop;
        this.hiraganaQuaternaryMode = hiraganaQuaternaryMode;
        this.collator = buildCollator();
        this.perThreadICUCollationField = ThreadLocal.withInitial(() -> new ICUCollationDocValuesField(name(), collator));
    }

    public Collator buildCollator() {
        Collator collator;
        if (rules != null) {
            try {
                collator = new RuleBasedCollator(rules);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse collation rules", e);
            }
        } else {
            if (language != null) {
                ULocale locale;
                if (country != null) {
                    if (variant != null) {
                        locale = new ULocale(language, country, variant);
                    } else {
                        locale = new ULocale(language, country);
                    }
                } else {
                    locale = new ULocale(language);
                }
                collator = Collator.getInstance(locale);
            } else {
                collator = Collator.getInstance();
            }
        }

        // set the strength flag, otherwise it will be the default.
        if (strength != null) {
            if (strength.equalsIgnoreCase("primary")) {
                collator.setStrength(Collator.PRIMARY);
            } else if (strength.equalsIgnoreCase("secondary")) {
                collator.setStrength(Collator.SECONDARY);
            } else if (strength.equalsIgnoreCase("tertiary")) {
                collator.setStrength(Collator.TERTIARY);
            } else if (strength.equalsIgnoreCase("quaternary")) {
                collator.setStrength(Collator.QUATERNARY);
            } else if (strength.equalsIgnoreCase("identical")) {
                collator.setStrength(Collator.IDENTICAL);
            } else {
                throw new IllegalArgumentException("Invalid strength: " + strength);
            }
        }

        // set the decomposition flag, otherwise it will be the default.
        if (decomposition != null) {
            if (decomposition.equalsIgnoreCase("no")) {
                collator.setDecomposition(Collator.NO_DECOMPOSITION);
            } else if (decomposition.equalsIgnoreCase("canonical")) {
                collator.setDecomposition(Collator.CANONICAL_DECOMPOSITION);
            } else {
                throw new IllegalArgumentException("Invalid decomposition: " + decomposition);
            }
        }

        // expert options: concrete subclasses are always a RuleBasedCollator
        RuleBasedCollator rbc = (RuleBasedCollator) collator;
        if (alternate != null) {
            if (alternate.equalsIgnoreCase("shifted")) {
                rbc.setAlternateHandlingShifted(true);
            } else if (alternate.equalsIgnoreCase("non-ignorable")) {
                rbc.setAlternateHandlingShifted(false);
            } else {
                throw new IllegalArgumentException("Invalid alternate: " + alternate);
            }
        }

        if (caseLevel) {
            rbc.setCaseLevel(true);
        }

        if (caseFirst != null) {
            if (caseFirst.equalsIgnoreCase("lower")) {
                rbc.setLowerCaseFirst(true);
            } else if (caseFirst.equalsIgnoreCase("upper")) {
                rbc.setUpperCaseFirst(true);
            } else {
                throw new IllegalArgumentException("Invalid caseFirst: " + caseFirst);
            }
        }

        if (numeric) {
            rbc.setNumericCollation(true);
        }

        if (variableTop != null) {
            rbc.setVariableTop(variableTop);
        }

        if (hiraganaQuaternaryMode) {
            rbc.setHiraganaQuaternary(true);
        }

        return collator;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        List<String> conflicts = new ArrayList<>();
        ICUCollationFieldMapper icuMergeWith = (ICUCollationFieldMapper) mergeWith;

        if (!Objects.equals(rules, icuMergeWith.rules)) {
            conflicts.add("Cannot update rules setting for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(language, icuMergeWith.language)) {
            conflicts.add("Cannot update language setting for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(country, icuMergeWith.country)) {
            conflicts.add("Cannot update country setting for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(variant, icuMergeWith.variant)) {
            conflicts.add("Cannot update variant setting for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(strength, icuMergeWith.strength)) {
            conflicts.add("Cannot update strength setting for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(decomposition, icuMergeWith.decomposition)) {
            conflicts.add("Cannot update decomposition setting for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(alternate, icuMergeWith.alternate)) {
            conflicts.add("Cannot update alternate setting for [" + CONTENT_TYPE + "]");
        }

        if (caseLevel != icuMergeWith.caseLevel) {
            conflicts.add("Cannot update caseLevel setting for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(caseFirst, icuMergeWith.caseFirst)) {
            conflicts.add("Cannot update caseFirst setting for [" + CONTENT_TYPE + "]");
        }

        if (numeric != icuMergeWith.numeric) {
            conflicts.add("Cannot update numeric setting for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(variableTop, icuMergeWith.variableTop)) {
            conflicts.add("Cannot update variableTop setting for [" + CONTENT_TYPE + "]");
        }

        if (hiraganaQuaternaryMode != icuMergeWith.hiraganaQuaternaryMode) {
            conflicts.add("Cannot update hiraganaQuaternaryMode setting for [" + CONTENT_TYPE + "]");
        }

        if (!conflicts.isEmpty()) {
            throw new IllegalArgumentException("Can't merge because of conflicts: " + conflicts);
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (rules != null) {
            builder.field("rules", rules);
        }

        if (language != null) {
            builder.field("language", language);
        }

        if (country != null) {
            builder.field("country", country);
        }

        if (variant != null) {
            builder.field("variant", variant);
        }

        if (strength != null) {
            builder.field("strength", strength);
        }

        if (decomposition != null) {
            builder.field("decomposition", decomposition);
        }

        if (alternate != null) {
            builder.field("alternate", alternate);
        }

        if (caseLevel) {
            builder.field("caseLevel", caseLevel);
        }

        if (caseFirst != null) {
            builder.field("caseFirst", caseFirst);
        }

        if (numeric) {
            builder.field("numeric", numeric);
        }

        if (variableTop != null) {
            builder.field("variableTop", variableTop);
        }

        if (hiraganaQuaternaryMode) {
            builder.field("hiraganaQuaternaryMode", hiraganaQuaternaryMode);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        final Object value;
        if (context.externalValueSet()) {
            value = context.externalValue();
        } else {
            value = context.parser().textOrNull();
        }

        if (value != null) {
            ICUCollationDocValuesField icuCollationField = perThreadICUCollationField.get();
            icuCollationField.setStringValue(Objects.toString(value));
            fields.add(icuCollationField);
        }
    }
}
