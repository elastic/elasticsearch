/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.util.Collections;
import java.util.Map;

public class RoutingFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_routing";
    public static final String CONTENT_TYPE = "_routing";

    @Override
    public FieldMapper.Builder getMergeBuilder() {
        return new Builder().init(this);
    }

    public static class Defaults {
        public static final boolean REQUIRED = false;
        public static final boolean DOC_VALUES = false;
    }

    private static RoutingFieldMapper toType(FieldMapper in) {
        return (RoutingFieldMapper) in;
    }

    public static class Builder extends MetadataFieldMapper.Builder {

        final Parameter<Boolean> required = Parameter.boolParam("required", false, m -> toType(m).required, Defaults.REQUIRED);
        final Parameter<Boolean> docValues = Parameter.boolParam("doc_values", false, m -> toType(m).docValues, Defaults.DOC_VALUES);

        protected Builder() {
            super(NAME);
        }

        @Override
        protected Parameter<?>[] getParameters() {
            return new Parameter<?>[] { required, docValues };
        }

        @Override
        public String contentType() {
            return CONTENT_TYPE;
        }

        @Override
        public RoutingFieldMapper build() {
            return RoutingFieldMapper.get(required.getValue(), docValues.getValue());
        }
    }

    public static final TypeParser PARSER = new ConfigurableTypeParser(c -> new Builder());

    /**
     * Field type used when routing is stored as a stored field (the default).
     */
    public static final MappedFieldType FIELD_TYPE = new RoutingFieldType(false);

    /**
     * Field type used when routing is stored as sorted doc values.
     */
    public static final MappedFieldType DOC_VALUES_FIELD_TYPE = new RoutingFieldType(true);

    static final class RoutingFieldType extends StringFieldType {

        private final boolean docValues;

        private RoutingFieldType(boolean docValues) {
            super(NAME, IndexType.terms(true, docValues), docValues == false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
            this.docValues = docValues;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (docValues) {
                return new DocValueFetcher(DocValueFormat.RAW, context.getForField(this, FielddataOperation.SEARCH));
            }
            return new StoredValueFetcher(context.lookup(), NAME);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            if (docValues) {
                return new SortedOrdinalsIndexFieldData.Builder(
                    name(),
                    CoreValuesSourceType.KEYWORD,
                    (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
                );
            }
            return super.fielddataBuilder(fieldDataContext);
        }
    }

    /**
     * Should we require {@code routing} on CRUD operations?
     */
    private final boolean required;

    /**
     * Whether routing values are stored as sorted doc values instead of stored fields.
     */
    private final boolean docValues;

    private static final RoutingFieldMapper REQUIRED_STORED = new RoutingFieldMapper(true, false);
    private static final RoutingFieldMapper NOT_REQUIRED_STORED = new RoutingFieldMapper(false, false);
    private static final RoutingFieldMapper REQUIRED_DOC_VALUES = new RoutingFieldMapper(true, true);
    private static final RoutingFieldMapper NOT_REQUIRED_DOC_VALUES = new RoutingFieldMapper(false, true);

    private static final Map<String, NamedAnalyzer> ANALYZERS = Map.of(NAME, Lucene.KEYWORD_ANALYZER);

    public static RoutingFieldMapper get(boolean required) {
        return get(required, Defaults.DOC_VALUES);
    }

    public static RoutingFieldMapper get(boolean required, boolean docValues) {
        if (docValues) {
            return required ? REQUIRED_DOC_VALUES : NOT_REQUIRED_DOC_VALUES;
        }
        return required ? REQUIRED_STORED : NOT_REQUIRED_STORED;
    }

    private RoutingFieldMapper(boolean required, boolean docValues) {
        super(docValues ? DOC_VALUES_FIELD_TYPE : FIELD_TYPE);
        this.required = required;
        this.docValues = docValues;
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return ANALYZERS;
    }

    /**
     * Should we require {@code routing} on CRUD operations?
     */
    public boolean required() {
        return this.required;
    }

    /**
     * Whether routing values are stored as sorted doc values instead of stored fields.
     * When {@code true}, routing is stored as sorted doc values with a skip index and no inverted index,
     * rather than a stored {@link StringField}, enabling sort and aggregation use cases.
     */
    public boolean docValues() {
        return this.docValues;
    }

    @Override
    public void preParse(DocumentParserContext context) {
        String routing = context.routing();
        if (routing != null) {
            if (docValues) {
                context.doc().add(SortedDocValuesField.indexedField(fieldType().name(), new BytesRef(routing)));
                // _field_names is only used for fields without doc values; doc values fields use FieldExistsQuery directly
            } else {
                context.doc().add(new StringField(fieldType().name(), routing, Field.Store.YES));
                context.addToFieldNames(fieldType().name());
            }
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
