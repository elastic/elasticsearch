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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
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

    public static final String NAME = "_ignored";

    public static final String CONTENT_TYPE = "_ignored";

    public static class Defaults {
        public static final String NAME = IgnoredFieldMapper.NAME;
    }

    public static final IgnoredFieldType FIELD_TYPE = new IgnoredFieldType();
    private static final IgnoredFieldMapper INSTANCE = new IgnoredFieldMapper(FIELD_TYPE);

    public static final LegacyIgnoredFieldType LEGACY_FIELD_TYPE = new LegacyIgnoredFieldType();
    private static final IgnoredFieldMapper LEGACY_INSTANCE = new IgnoredFieldMapper(LEGACY_FIELD_TYPE);

    public static final TypeParser PARSER = new FixedTypeParser(c -> getInstance(c.indexVersionCreated()));

    private static MetadataFieldMapper getInstance(IndexVersion indexVersion) {
        return indexVersion.onOrAfter(IndexVersions.DOC_VALUES_FOR_IGNORED_META_FIELD) ? INSTANCE : LEGACY_INSTANCE;
    }

    public static final class LegacyIgnoredFieldType extends StringFieldType {

        private LegacyIgnoredFieldType() {
            super(NAME, true, true, false, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new StoredValueFetcher(context.lookup(), NAME);
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            throw new IllegalArgumentException(
                "aggregations on the '"
                    + typeName()
                    + "' field are supported for indices created by version "
                    + IndexVersions.DOC_VALUES_FOR_IGNORED_META_FIELD
                    + " or higher"
            );
        }
    }

    public static final class IgnoredFieldType extends StringFieldType {

        private IgnoredFieldType() {
            super(NAME, true, false, true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            if (context.getIndexSettings().getIndexVersionCreated().before(IndexVersions.DOC_VALUES_FOR_IGNORED_META_FIELD)) {
                return new StoredValueFetcher(context.lookup(), NAME);
            }
            return new DocValueFetcher(docValueFormat(format, null), context.getForField(this, FielddataOperation.SEARCH));
        }

        public Query existsQuery(SearchExecutionContext context) {
            if (hasDocValues() || getTextSearchInfo().hasNorms()) {
                return new FieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(IgnoredFieldMapper.NAME, name()));
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(FieldDataContext fieldDataContext) {
            return new SortedSetOrdinalsIndexFieldData.Builder(
                name(),
                CoreValuesSourceType.KEYWORD,
                (dv, n) -> new KeywordDocValuesField(FieldData.toString(dv), n)
            );
        }
    }

    private IgnoredFieldMapper(StringFieldType fieldType) {
        super(fieldType);
    }

    @Override
    public void postParse(DocumentParserContext context) {
        for (String ignoredField : context.getIgnoredFields()) {
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), new BytesRef(ignoredField)));
            context.doc().add(new StringField(fieldType().name(), ignoredField, Field.Store.NO));
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
