/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.sandbox.search.DocValuesTermsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collection;
import java.util.Collections;

/**
 * Field mapper to access the legacy _type that existed in Elasticsearch 5
 */
public class LegacyTypeFieldMapper extends MetadataFieldMapper {

    public static final String NAME = "_type";

    public static final String CONTENT_TYPE = "_type";

    private static final LegacyTypeFieldMapper INSTANCE = new LegacyTypeFieldMapper();

    public static final TypeParser PARSER = new FixedTypeParser(c -> INSTANCE);

    protected LegacyTypeFieldMapper() {
        super(new LegacyTypeFieldType(), Lucene.KEYWORD_ANALYZER);
    }

    static final class LegacyTypeFieldType extends TermBasedFieldType {

        LegacyTypeFieldType() {
            super(NAME, false, true, true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            // The _type field is always searchable.
            return true;
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return SortedSetDocValuesField.newSlowExactQuery(name(), indexedValueForSearch(value));
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            BytesRef[] bytesRefs = values.stream().map(this::indexedValueForSearch).toArray(BytesRef[]::new);
            return new DocValuesTermsQuery(name(), bytesRefs);
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            SearchExecutionContext context
        ) {
            return SortedSetDocValuesField.newSlowRangeQuery(
                name(),
                lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                upperTerm == null ? null : indexedValueForSearch(upperTerm),
                includeLower,
                includeUpper
            );
        }

        @Override
        public boolean mayExistInIndex(SearchExecutionContext context) {
            return true;
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new StoredValueFetcher(context.lookup(), NAME);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
