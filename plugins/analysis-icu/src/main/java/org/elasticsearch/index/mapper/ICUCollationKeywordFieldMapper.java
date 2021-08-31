/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RawCollationKey;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.IndexableBinaryStringTools;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ICUCollationKeywordFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "icu_collation_keyword";

    public static final class CollationFieldType extends StringFieldType {
        private final Collator collator;
        private final String nullValue;
        private final int ignoreAbove;

        public CollationFieldType(String name, boolean isSearchable, boolean isStored, boolean hasDocValues,
                                  Collator collator, String nullValue, int ignoreAbove, Map<String, String> meta) {
            super(name, isSearchable, isStored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.collator = collator;
            this.nullValue = nullValue;
            this.ignoreAbove = ignoreAbove;
        }

        public CollationFieldType(String name, boolean searchable, Collator collator) {
            this(name, searchable, false, true, collator, null, Integer.MAX_VALUE, Collections.emptyMap());
        }

        public CollationFieldType(String name, Collator collator) {
            this(name, true, false, true, collator, null, Integer.MAX_VALUE, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            return new SourceValueFetcher(name(), context, nullValue) {
                @Override
                protected String parseSourceValue(Object value) {
                    String keywordValue = value.toString();
                    if (keywordValue.length() > ignoreAbove) {
                        return null;
                    }
                    return keywordValue;
                }
            };
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.KEYWORD);
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
                                boolean transpositions, SearchExecutionContext context) {
            throw new UnsupportedOperationException("[fuzzy] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method,
                                 boolean caseInsensitive, SearchExecutionContext context) {
            throw new UnsupportedOperationException("[prefix] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query wildcardQuery(String value,
                                   @Nullable MultiTermQuery.RewriteMethod method,
                                   boolean caseInsensitive,
                                   SearchExecutionContext context) {
            throw new UnsupportedOperationException("[wildcard] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query regexpQuery(String value, int syntaxFlags, int matchFlags, int maxDeterminizedStates,
                                 MultiTermQuery.RewriteMethod method, SearchExecutionContext context) {
            throw new UnsupportedOperationException("[regexp] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        public static final DocValueFormat COLLATE_FORMAT = new DocValueFormat() {
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

    private static ICUCollationKeywordFieldMapper toType(FieldMapper in) {
        return (ICUCollationKeywordFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);
        final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).fieldType.stored(), false);

        final Parameter<String> indexOptions
            = Parameter.restrictedStringParam("index_options", false, m -> toType(m).indexOptions, "docs", "freqs");
        final Parameter<Boolean> hasNorms = TextParams.norms(false, m -> toType(m).fieldType.omitNorms() == false);

        final Parameter<Map<String, String>> meta = Parameter.metaParam();

        final Parameter<String> rules
            = Parameter.stringParam("rules", false, m -> toType(m).params.rules, null).acceptsNull();
        final Parameter<String> language
            = Parameter.stringParam("language", false, m -> toType(m).params.language, null).acceptsNull();
        final Parameter<String> country
            = Parameter.stringParam("country", false, m -> toType(m).params.country, null).acceptsNull();
        final Parameter<String> variant
            = Parameter.stringParam("variant", false, m -> toType(m).params.variant, null).acceptsNull();
        final Parameter<String> strength
            = Parameter.stringParam("strength", false, m -> toType(m).params.strength, null).acceptsNull();
        final Parameter<String> decomposition
            = Parameter.stringParam("decomposition", false, m -> toType(m).params.decomposition, null)
            .acceptsNull();
        final Parameter<String> alternate
            = Parameter.stringParam("alternate", false, m -> toType(m).params.alternate, null).acceptsNull();
        final Parameter<Boolean> caseLevel = Parameter.boolParam("case_level", false, m -> toType(m).params.caseLevel, false);
        final Parameter<String> caseFirst
            = Parameter.stringParam("case_first", false, m -> toType(m).params.caseFirst, null).acceptsNull();
        final Parameter<Boolean> numeric = Parameter.boolParam("numeric", false, m -> toType(m).params.numeric, false);
        final Parameter<String> variableTop
            = Parameter.stringParam("variable_top", false, m -> toType(m).params.variableTop, null).acceptsNull();
        final Parameter<Boolean> hiraganaQuaternaryMode
            = Parameter.boolParam("hiragana_quaternary_mode", false, m -> toType(m).params.hiraganaQuaternaryMode, false).acceptsNull();

        final Parameter<Integer> ignoreAbove
            = Parameter.intParam("ignore_above", true, m -> toType(m).ignoreAbove, Integer.MAX_VALUE)
            .addValidator(v -> {
                if (v < 0) {
                    throw new IllegalArgumentException("[ignore_above] must be positive, got [" + v + "]");
                }
            });
        final Parameter<String> nullValue
            = Parameter.stringParam("null_value", false, m -> toType(m).nullValue, null).acceptsNull();

        public Builder(String name) {
            super(name);
        }

        Builder nullValue(String nullValue) {
            this.nullValue.setValue(nullValue);
            return this;
        }

        Builder ignoreAbove(int ignoreAbove) {
            this.ignoreAbove.setValue(ignoreAbove);
            return this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(indexed, hasDocValues, stored, indexOptions, hasNorms,
                rules, language, country, variant, strength, decomposition, alternate,
                caseLevel, caseFirst, numeric, variableTop, hiraganaQuaternaryMode,
                ignoreAbove, nullValue, meta);
        }

        private CollatorParams collatorParams() {
            CollatorParams params = new CollatorParams();
            params.rules = rules.getValue();
            params.language = language.getValue();
            params.country = country.getValue();
            params.variant = variant.getValue();
            params.strength = strength.getValue();
            params.decomposition = decomposition.getValue();
            params.alternate = alternate.getValue();
            params.caseLevel = caseLevel.getValue();
            params.caseFirst = caseFirst.getValue();
            params.numeric = numeric.getValue();
            params.variableTop = variableTop.getValue();
            params.hiraganaQuaternaryMode = hiraganaQuaternaryMode.getValue();
            return params;
        }

        private FieldType buildFieldType() {
            FieldType ft = new FieldType();
            ft.setTokenized(false);
            ft.setOmitNorms(hasNorms.getValue() == false);
            ft.setIndexOptions(TextParams.toIndexOptions(indexed.getValue(), indexOptions.getValue()));
            ft.setStored(stored.getValue());
            return ft;
        }

        @Override
        public ICUCollationKeywordFieldMapper build(ContentPath contentPath) {
            final CollatorParams params = collatorParams();
            final Collator collator = params.buildCollator();
            CollationFieldType ft = new CollationFieldType(buildFullName(contentPath), indexed.getValue(),
                stored.getValue(), hasDocValues.getValue(), collator, nullValue.getValue(), ignoreAbove.getValue(),
                meta.getValue());
            return new ICUCollationKeywordFieldMapper(name, buildFieldType(), ft,
                multiFieldsBuilder.build(this, contentPath), copyTo.build(), collator, this);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    private static class CollatorParams {
        private String rules;
        private String language;
        private String country;
        private String variant;
        private String strength;
        private String decomposition;
        private String alternate;
        private boolean caseLevel;
        private String caseFirst;
        private boolean numeric;
        private String variableTop;
        private boolean hiraganaQuaternaryMode;

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
    }

    private final int ignoreAbove;
    private final Collator collator;
    private final CollatorParams params;
    private final String nullValue;
    private final FieldType fieldType;
    private final boolean indexed;
    private final boolean hasDocValues;
    private final String indexOptions;

    protected ICUCollationKeywordFieldMapper(String simpleName, FieldType fieldType,
                                             MappedFieldType mappedFieldType,
                                             MultiFields multiFields, CopyTo copyTo,
                                             Collator collator, Builder builder) {
        super(simpleName, mappedFieldType, Lucene.KEYWORD_ANALYZER, multiFields, copyTo);
        assert collator.isFrozen();
        this.fieldType = fieldType;
        this.params = builder.collatorParams();
        this.ignoreAbove = builder.ignoreAbove.getValue();
        this.collator = collator;
        this.nullValue = builder.nullValue.getValue();
        this.indexed = builder.indexed.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.indexOptions = builder.indexOptions.getValue();
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
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        final String value;
        XContentParser parser = context.parser();
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            value = nullValue;
        } else {
            value = parser.textOrNull();
        }

        if (value == null) {
            return;
        }

        if (value.length() > ignoreAbove) {
            context.addIgnoredField(name());
            return;
        }

        RawCollationKey key = collator.getRawCollationKey(value, null);
        final BytesRef binaryValue = new BytesRef(key.bytes, 0, key.size);

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new Field(mappedFieldType.name(), binaryValue, fieldType);
            context.doc().add(field);
        }

        if (hasDocValues) {
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
        } else if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            context.addToFieldNames(fieldType().name());
        }
    }

}
