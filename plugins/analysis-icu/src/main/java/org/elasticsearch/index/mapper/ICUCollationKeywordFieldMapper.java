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
import com.ibm.icu.text.RawCollationKey;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.analysis.IndexableBinaryStringTools;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ICUCollationKeywordFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "icu_collation_keyword";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new CollationFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }

        public static final String NULL_VALUE = null;
        public static final int IGNORE_ABOVE = Integer.MAX_VALUE;
    }

    public static final class CollationFieldType extends StringFieldType {
        private Collator collator = null;

        public CollationFieldType() {
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            setSearchAnalyzer(Lucene.KEYWORD_ANALYZER);
        }

        protected CollationFieldType(CollationFieldType ref) {
            super(ref);
            this.collator = ref.collator;
        }

        @Override
        public CollationFieldType clone() {
            return new CollationFieldType(this);
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o) && Objects.equals(collator, ((CollationFieldType) o).collator);
        }

        @Override
        public void checkCompatibility(MappedFieldType otherFT, List<String> conflicts) {
            super.checkCompatibility(otherFT, conflicts);
            CollationFieldType other = (CollationFieldType) otherFT;
            if (!Objects.equals(collator, other.collator)) {
                conflicts.add("mapper [" + name() + "] has different [collator]");
            }
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + Objects.hashCode(collator);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public Collator collator() {
            return collator;
        }

        public void setCollator(Collator collator) {
            checkIfFrozen();
            this.collator = collator.isFrozen() ? collator : collator.freeze();
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder();
        }

        @Override
        protected BytesRef indexedValueForSearch(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }

            if (collator != null) {
                RawCollationKey key = collator.getRawCollationKey(value.toString(), null);
                return new BytesRef(key.bytes, 0, key.size);
            } else {
                throw new IllegalStateException("collator is null");
            }
        }

        @Override
        public Query fuzzyQuery(Object value, Fuzziness fuzziness, int prefixLength, int maxExpansions,
                                boolean transpositions) {
            throw new UnsupportedOperationException("[fuzzy] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            throw new UnsupportedOperationException("[prefix] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query wildcardQuery(String value,
                                   @Nullable MultiTermQuery.RewriteMethod method,
                                   QueryShardContext context) {
            throw new UnsupportedOperationException("[wildcard] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query regexpQuery(String value, int flags, int maxDeterminizedStates,
                                 MultiTermQuery.RewriteMethod method, QueryShardContext context) {
            throw new UnsupportedOperationException("[regexp] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        public static DocValueFormat COLLATE_FORMAT = new DocValueFormat() {
            @Override
            public String getWriteableName() {
                return "collate";
            }

            @Override
            public void writeTo(StreamOutput out) {
            }

            @Override
            public String format(BytesRef value) {
                int encodedLength = IndexableBinaryStringTools.getEncodedLength(value.bytes, value.offset, value.length);
                char[] encoded = new char[encodedLength];
                IndexableBinaryStringTools.encode(value.bytes, value.offset, value.length, encoded, 0, encodedLength);
                return new String(encoded, 0, encodedLength);
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
        public DocValueFormat docValueFormat(final String format, final ZoneId timeZone) {
            return COLLATE_FORMAT;
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, ICUCollationKeywordFieldMapper> {
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
        protected int ignoreAbove = Defaults.IGNORE_ABOVE;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public CollationFieldType fieldType() {
            return (CollationFieldType) super.fieldType();
        }

        @Override
        public Builder indexOptions(IndexOptions indexOptions) {
            if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) > 0) {
                throw new IllegalArgumentException("The [" + CONTENT_TYPE + "] field does not support positions, got [index_options]="
                    + indexOptionToString(indexOptions));
            }

            return super.indexOptions(indexOptions);
        }

        public Builder ignoreAbove(int ignoreAbove) {
            if (ignoreAbove < 0) {
                throw new IllegalArgumentException("[ignore_above] must be positive, got " + ignoreAbove);
            }
            this.ignoreAbove = ignoreAbove;
            return this;
        }

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
                    collator = Collator.getInstance(ULocale.ROOT);
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

            // freeze so thread-safe
            return collator.freeze();
        }

        @Override
        public ICUCollationKeywordFieldMapper build(BuilderContext context) {
            final Collator collator = buildCollator();
            fieldType().setCollator(collator);
            setupFieldType(context);
            return new ICUCollationKeywordFieldMapper(name, fieldType, defaultFieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), copyTo, rules, language, country, variant, strength, decomposition,
                alternate, caseLevel, caseFirst, numeric, variableTop, hiraganaQuaternaryMode, ignoreAbove, collator);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext)
            throws MapperParsingException {
            Builder builder = new Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext(); ) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                switch (fieldName) {
                    case "null_value":
                        if (fieldNode == null) {
                            throw new MapperParsingException("Property [null_value] cannot be null.");
                        }
                        builder.nullValue(fieldNode.toString());
                        iterator.remove();
                        break;
                    case "ignore_above":
                        builder.ignoreAbove(XContentMapValues.nodeIntegerValue(fieldNode, -1));
                        iterator.remove();
                        break;
                    case "norms":
                        builder.omitNorms(!XContentMapValues.nodeBooleanValue(fieldNode, "norms"));
                        iterator.remove();
                        break;
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
                    case "case_level":
                        builder.caseLevel(XContentMapValues.nodeBooleanValue(fieldNode, false));
                        iterator.remove();
                        break;
                    case "case_first":
                        builder.caseFirst(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "numeric":
                        builder.numeric(XContentMapValues.nodeBooleanValue(fieldNode, false));
                        iterator.remove();
                        break;
                    case "variable_top":
                        builder.variableTop(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "hiragana_quaternary_mode":
                        builder.hiraganaQuaternaryMode(XContentMapValues.nodeBooleanValue(fieldNode, false));
                        iterator.remove();
                        break;
                    default:
                        break;
                }
            }

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
    private int ignoreAbove;
    private final Collator collator;

    protected ICUCollationKeywordFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                                             Settings indexSettings, MultiFields multiFields, CopyTo copyTo, String rules, String language,
                                             String country, String variant,
                                             String strength, String decomposition, String alternate, boolean caseLevel, String caseFirst,
                                             boolean numeric, String variableTop, boolean hiraganaQuaternaryMode,
                                             int ignoreAbove, Collator collator) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        assert collator.isFrozen();
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
        this.ignoreAbove = ignoreAbove;
        this.collator = collator;
    }

    @Override
    public CollationFieldType fieldType() {
        return (CollationFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doMerge(Mapper mergeWith) {
        super.doMerge(mergeWith);

        List<String> conflicts = new ArrayList<>();
        ICUCollationKeywordFieldMapper icuMergeWith = (ICUCollationKeywordFieldMapper) mergeWith;

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
            conflicts.add("Cannot update case_level setting for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(caseFirst, icuMergeWith.caseFirst)) {
            conflicts.add("Cannot update case_first setting for [" + CONTENT_TYPE + "]");
        }

        if (numeric != icuMergeWith.numeric) {
            conflicts.add("Cannot update numeric setting for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(variableTop, icuMergeWith.variableTop)) {
            conflicts.add("Cannot update variable_top setting for [" + CONTENT_TYPE + "]");
        }

        if (hiraganaQuaternaryMode != icuMergeWith.hiraganaQuaternaryMode) {
            conflicts.add("Cannot update hiragana_quaternary_mode setting for [" + CONTENT_TYPE + "]");
        }

        this.ignoreAbove = icuMergeWith.ignoreAbove;

        if (!conflicts.isEmpty()) {
            throw new IllegalArgumentException("Can't merge because of conflicts: " + conflicts);
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);

        if (includeDefaults || fieldType().nullValue() != null) {
            builder.field("null_value", fieldType().nullValue());
        }

        if (includeDefaults || rules != null) {
            builder.field("rules", rules);
        }

        if (includeDefaults || language != null) {
            builder.field("language", language);
        }

        if (includeDefaults || country != null) {
            builder.field("country", country);
        }

        if (includeDefaults || variant != null) {
            builder.field("variant", variant);
        }

        if (includeDefaults || strength != null) {
            builder.field("strength", strength);
        }

        if (includeDefaults || decomposition != null) {
            builder.field("decomposition", decomposition);
        }

        if (includeDefaults || alternate != null) {
            builder.field("alternate", alternate);
        }

        if (includeDefaults || caseLevel) {
            builder.field("case_level", caseLevel);
        }

        if (includeDefaults || caseFirst != null) {
            builder.field("case_first", caseFirst);
        }

        if (includeDefaults || numeric) {
            builder.field("numeric", numeric);
        }

        if (includeDefaults || variableTop != null) {
            builder.field("variable_top", variableTop);
        }

        if (includeDefaults || hiraganaQuaternaryMode) {
            builder.field("hiragana_quaternary_mode", hiraganaQuaternaryMode);
        }

        if (includeDefaults || ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        final String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                value = fieldType().nullValueAsString();
            } else {
                value = parser.textOrNull();
            }
        }

        if (value == null || value.length() > ignoreAbove) {
            return;
        }

        RawCollationKey key = collator.getRawCollationKey(value, null);
        final BytesRef binaryValue = new BytesRef(key.bytes, 0, key.size);

        if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            Field field = new Field(fieldType().name(), binaryValue, fieldType());
            fields.add(field);
        }

        if (fieldType().hasDocValues()) {
            fields.add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
        } else if (fieldType().indexOptions() != IndexOptions.NONE || fieldType().stored()) {
            createFieldNamesField(context, fields);
        }
    }
}
