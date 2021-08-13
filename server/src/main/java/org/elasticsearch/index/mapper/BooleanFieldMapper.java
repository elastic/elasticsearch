/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.elasticsearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.FieldValues;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A field mapper for boolean fields.
 */
public class BooleanFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "boolean";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.freeze();
        }
    }

    public static class Values {
        public static final BytesRef TRUE = new BytesRef("T");
        public static final BytesRef FALSE = new BytesRef("F");
    }

    private static BooleanFieldMapper toType(FieldMapper in) {
        return (BooleanFieldMapper) in;
    }

    public static class Builder extends FieldMapper.Builder {

        private final Parameter<Boolean> docValues = Parameter.docValuesParam(m -> toType(m).hasDocValues,  true);
        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);

        private final Parameter<Boolean> nullValue = new Parameter<>("null_value", false, () -> null,
            (n, c, o) -> o == null ? null : XContentMapValues.nodeBooleanValue(o), m -> toType(m).nullValue)
            .acceptsNull();

        private final Parameter<Script> script = Parameter.scriptParam(m -> toType(m).script);
        private final Parameter<String> onScriptError = Parameter.onScriptErrorParam(m -> toType(m).onScriptError, script);

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final ScriptCompiler scriptCompiler;

        public Builder(String name, ScriptCompiler scriptCompiler) {
            super(name);
            this.scriptCompiler = Objects.requireNonNull(scriptCompiler);
            this.script.precludesParameters(nullValue);
            addScriptValidation(script, indexed, docValues);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(meta, docValues, indexed, nullValue, stored, script, onScriptError);
        }

        @Override
        public BooleanFieldMapper build(ContentPath contentPath) {
            MappedFieldType ft = new BooleanFieldType(buildFullName(contentPath), indexed.getValue(), stored.getValue(),
                docValues.getValue(), nullValue.getValue(), scriptValues(), meta.getValue());

            return new BooleanFieldMapper(name, ft, multiFieldsBuilder.build(this, contentPath), copyTo.build(), this);
        }

        private FieldValues<Boolean> scriptValues() {
            if (script.get() == null) {
                return null;
            }
            BooleanFieldScript.Factory scriptFactory = scriptCompiler.compile(script.get(), BooleanFieldScript.CONTEXT);
            return scriptFactory == null ? null : (lookup, ctx, doc, consumer) -> scriptFactory
                .newFactory(name, script.get().getParams(), lookup)
                .newInstance(ctx)
                .runForDoc(doc, consumer);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.scriptCompiler()));

    public static final class BooleanFieldType extends TermBasedFieldType {

        private final Boolean nullValue;
        private final FieldValues<Boolean> scriptValues;

        public BooleanFieldType(String name, boolean isSearchable, boolean isStored, boolean hasDocValues,
                                Boolean nullValue, FieldValues<Boolean> scriptValues, Map<String, String> meta) {
            super(name, isSearchable, isStored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.nullValue = nullValue;
            this.scriptValues = scriptValues;
        }

        public BooleanFieldType(String name) {
            this(name, true, false, true, false, null, Collections.emptyMap());
        }

        public BooleanFieldType(String name, boolean searchable) {
            this(name, searchable, false, true, false, null, Collections.emptyMap());
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
            if (this.scriptValues != null) {
                return FieldValues.valueFetcher(this.scriptValues, context);
            }
            return new SourceValueFetcher(name(), context, nullValue) {
                @Override
                protected Boolean parseSourceValue(Object value) {
                    if (value instanceof Boolean) {
                        return (Boolean) value;
                    } else {
                        String textValue = value.toString();
                        return Booleans.parseBoolean(textValue.toCharArray(), 0, textValue.length(), false);
                    }
                }
            };
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            if (value == null) {
                return Values.FALSE;
            }
            if (value instanceof Boolean) {
                return ((Boolean) value) ? Values.TRUE : Values.FALSE;
            }
            String sValue;
            if (value instanceof BytesRef) {
                sValue = ((BytesRef) value).utf8ToString();
            } else {
                sValue = value.toString();
            }
            switch (sValue) {
                case "true":
                    return Values.TRUE;
                case "false":
                    return Values.FALSE;
                default:
                    throw new IllegalArgumentException("Can't parse boolean value [" +
                                    sValue + "], expected [true] or [false]");
            }
        }

        @Override
        public Boolean valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            switch(value.toString()) {
            case "F":
                return false;
            case "T":
                return true;
            default:
                throw new IllegalArgumentException("Expected [T] or [F] but got [" + value + "]");
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(name(), NumericType.BOOLEAN);
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
            }
            if (timeZone != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName()
                    + "] does not support custom time zones");
            }
            return DocValueFormat.BOOLEAN;
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper,
                                SearchExecutionContext context) {
            failIfNotIndexed();
            return new TermRangeQuery(name(),
                lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                upperTerm == null ? null : indexedValueForSearch(upperTerm),
                includeLower, includeUpper);
        }
    }

    private final Boolean nullValue;
    private final boolean indexed;
    private final boolean hasDocValues;
    private final boolean stored;
    private final Script script;
    private final FieldValues<Boolean> scriptValues;
    private final ScriptCompiler scriptCompiler;

    protected BooleanFieldMapper(String simpleName, MappedFieldType mappedFieldType,
                                 MultiFields multiFields, CopyTo copyTo, Builder builder) {
        super(simpleName, mappedFieldType, Lucene.KEYWORD_ANALYZER, multiFields, copyTo,
            builder.script.get() != null, builder.onScriptError.getValue());
        this.nullValue = builder.nullValue.getValue();
        this.stored = builder.stored.getValue();
        this.indexed = builder.indexed.getValue();
        this.hasDocValues = builder.docValues.getValue();
        this.script = builder.script.get();
        this.scriptValues = builder.scriptValues();
        this.scriptCompiler = builder.scriptCompiler;
    }

    @Override
    public BooleanFieldType fieldType() {
        return (BooleanFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) throws IOException {
        if (indexed == false && stored == false && hasDocValues == false) {
            return;
        }

        Boolean value = null;
        XContentParser.Token token = context.parser().currentToken();
        if (token == XContentParser.Token.VALUE_NULL) {
            if (nullValue != null) {
                value = nullValue;
            }
        } else {
            value = context.parser().booleanValue();
        }
        indexValue(context, value);
    }

    private void indexValue(DocumentParserContext context, Boolean value) {
        if (value == null) {
            return;
        }
        if (indexed) {
            context.doc().add(new Field(fieldType().name(), value ? "T" : "F", Defaults.FIELD_TYPE));
        }
        if (stored) {
            context.doc().add(new StoredField(fieldType().name(), value ? "T" : "F"));
        }
        if (hasDocValues) {
            context.doc().add(new SortedNumericDocValuesField(fieldType().name(), value ? 1 : 0));
        } else {
            context.addToFieldNames(fieldType().name());
        }
    }

    @Override
    protected void indexScriptValues(SearchLookup searchLookup, LeafReaderContext readerContext, int doc,
                                     DocumentParserContext documentParserContext) {
        this.scriptValues.valuesForDoc(searchLookup, readerContext, doc, value -> indexValue(documentParserContext, value));
    }

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), scriptCompiler).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
