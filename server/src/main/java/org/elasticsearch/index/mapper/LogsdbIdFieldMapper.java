/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Collection;
import java.util.Collections;

/**
 * A mapper for the _id field.
 */
public class LogsdbIdFieldMapper extends IdFieldMapper {
    // here
    public static final String NAME = "_id";

    public static final String CONTENT_TYPE = "_id";

    public static final LogsdbIdFieldMapper INSTANCE = new LogsdbIdFieldMapper();

    public LogsdbIdFieldMapper() {
        super(new LogsdbIdFieldType());
    }

    @Override
    public void preParse(DocumentParserContext context) {
        if (context.sourceToParse().id() == null) {
            throw new IllegalStateException("_id should have been set on the coordinating node");
        }
        context.id(context.sourceToParse().id());
        context.doc().add(new SortedDocValuesField(fieldType().name(), new BytesRef(context.id())));
    }

    @Override
    public String documentDescription(DocumentParserContext context) {
        return "logsdb document with id '" + context.sourceToParse().id() + "'";
    }

    @Override
    public String documentDescription(ParsedDocument parsedDocument) {
        return "[" + parsedDocument.id() + "]";
    }

    @Override
    public String reindexId(String id) {
        return id;
    }

    protected static class LogsdbIdFieldType extends TermBasedFieldType {

        public LogsdbIdFieldType() {
            super(NAME, false, false, true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            // searchable, but probably not fast
            return true;
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            var bytesRefs = values.stream().map(this::indexedValueForSearch).toList();
            return SortedSetDocValuesField.newSlowSetQuery(name(), bytesRefs);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return SortedSetDocValuesField.newSlowExactQuery(name(), indexedValueForSearch(value));
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            return new BlockDocValuesReader.BytesRefsFromOrdsBlockLoader(NAME);
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            // TODO can this be done somehow?
            throw new UnsupportedOperationException("logsdb id cannot be fetched by values since only using doc values");
        }
    }
}
