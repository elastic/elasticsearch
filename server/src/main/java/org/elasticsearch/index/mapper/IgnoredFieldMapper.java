/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.field.KeywordDocValuesField;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;

import java.util.Collections;

/**
 * A field mapper that records fields that have been ignored because they were malformed.
 */
public final class IgnoredFieldMapper extends MetadataFieldMapper {

    public static final IndexVersion AGGS_SUPPORT_VERSION = IndexVersion.fromId(Version.V_8_12_0.id());

    public static final String NAME = "_ignored";

    public static final String CONTENT_TYPE = "_ignored";

    public static class Defaults {
        public static final String NAME = IgnoredFieldMapper.NAME;
    }

    public static final IgnoredFieldType FIELD_TYPE = new IgnoredFieldType(true);
    private static final IgnoredFieldMapper INSTANCE = new IgnoredFieldMapper(FIELD_TYPE);

    public static final IgnoredFieldType LEGACY_FIELD_TYPE = new IgnoredFieldType(false);
    private static final IgnoredFieldMapper LEGACY_INSTANCE = new IgnoredFieldMapper(LEGACY_FIELD_TYPE);


    public static final TypeParser PARSER = new FixedTypeParser(c -> getInstance(c.indexVersionCreated()));

    public static MetadataFieldMapper getInstance(IndexVersion indexVersion) {
        return indexVersion.onOrAfter(AGGS_SUPPORT_VERSION)
            ? INSTANCE
            : LEGACY_INSTANCE;
    }

    public static final class IgnoredFieldType extends StringFieldType {

        private IgnoredFieldType(boolean hasDocValues) {
            super(NAME, true, true, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            // This query is not performance sensitive, it only helps assess
            // quality of the data, so we may use a slow query. It shouldn't
            // be too slow in practice since the number of unique terms in this
            // field is bounded by the number of fields in the mappings.
            return new TermRangeQuery(name(), null, null, true, true);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new StoredValueFetcher(context.lookup(), NAME);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            if (hasDocValues() == false) {
                throw new IllegalArgumentException(
                    "aggregations for the '" + typeName()  + "' field are supported from version + " + AGGS_SUPPORT_VERSION
                );
            }
            return new SortedSetOrdinalsIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
            );
        }
    }

    private IgnoredFieldMapper(IgnoredFieldType fieldType) {
        super(fieldType);
    }

    @Override
    public void postParse(DocumentParserContext context) {
        MappedFieldType mappedFieldType = fieldType();
        for (String field : context.getIgnoredFields()) {
            if (mappedFieldType.hasDocValues()) {
                final BytesRef binaryValue = new BytesRef(field);
                context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
            }
            context.doc().add(new StringField(NAME, field, Field.Store.YES));
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
    }
}
