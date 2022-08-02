/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.DelegateDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A field mapper used internally by the {@link ParentJoinFieldMapper} to index
 * the value that link documents in the index (parent _id or _id if the document is a parent).
 */
public final class ParentIdFieldMapper extends FieldMapper {
    static final String CONTENT_TYPE = "parent";

    static class Defaults {
        static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }
    }

    public static final class ParentIdFieldType extends StringFieldType {

        private final boolean eagerGlobalOrdinals;

        public ParentIdFieldType(String name, boolean eagerGlobalOrdinals) {
            super(name, true, false, true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
            this.eagerGlobalOrdinals = eagerGlobalOrdinals;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean eagerGlobalOrdinals() {
            return eagerGlobalOrdinals;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new DelegateDocValuesField(
                    new ScriptDocValues.Strings(new ScriptDocValues.StringsSupplier(FieldData.toString(dv))),
                    n
                )
            );
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            // Although this is an internal field, we return it in the list of all field types. So we
            // provide an empty value fetcher here instead of throwing an error.
            return (lookup, ignoredValues) -> List.of();
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }
    }

    protected ParentIdFieldMapper(String name, boolean eagerGlobalOrdinals) {
        super(name, new ParentIdFieldType(name, eagerGlobalOrdinals), MultiFields.empty(), CopyTo.empty(), false, null);
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return Map.of(mappedFieldType.name(), Lucene.KEYWORD_ANALYZER);
    }

    @Override
    protected void parseCreateField(DocumentParserContext context) {
        throw new UnsupportedOperationException("Cannot directly call parse() on a ParentIdFieldMapper");
    }

    public void indexValue(DocumentParserContext context, String refId) {
        BytesRef binaryValue = new BytesRef(refId);
        Field field = new Field(fieldType().name(), binaryValue, Defaults.FIELD_TYPE);
        context.doc().add(field);
        context.doc().add(new SortedDocValuesField(fieldType().name(), binaryValue));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public Builder getMergeBuilder() {
        return null;    // always constructed by ParentJoinFieldMapper, not through type parsers
    }
}
