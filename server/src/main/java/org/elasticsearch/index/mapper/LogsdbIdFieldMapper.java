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
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A mapper for the _id field.
 */
public abstract class LogsdbIdFieldMapper extends MetadataFieldMapper {
    // here
    public static final String NAME = "_id";

    public static final String CONTENT_TYPE = "_id";

    public static final TypeParser PARSER = new FixedTypeParser(MappingParserContext::idFieldMapper);

    private static final Map<String, NamedAnalyzer> ANALYZERS = Map.of(NAME, Lucene.KEYWORD_ANALYZER);

    public static final LogsdbIdFieldMapper INSTANCE = new LogsdbIdFieldMapper();

    protected LogsdbIdFieldMapper(MappedFieldType mappedFieldType) {
        super(mappedFieldType);
        assert mappedFieldType.isSearchable();
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return ANALYZERS;
    }

    @Override
    protected final String contentType() {
        return CONTENT_TYPE;
    }

    /**
     * Description of the document being parsed used in error messages. Not
     * called unless there is an error.
     */
    public abstract String documentDescription(DocumentParserContext context);

    /**
     * Description of the document being indexed used after parsing for things
     * like version conflicts.
     */
    public abstract String documentDescription(ParsedDocument parsedDocument);

    /**
     * Build the {@code _id} to use on requests reindexing into indices using
     * this {@code _id}.
     */
    public abstract String reindexId(String id);

    /**
     * Create a {@link Field} to store the provided {@code _id} that "stores"
     * the {@code _id} so it can be fetched easily from the index.
     */
    public static Field standardIdField(String id) {
        return new StringField(NAME, Uid.encodeId(id), Field.Store.NO);
    }

    protected abstract static class AbstractIdFieldType extends TermBasedFieldType {

        public AbstractIdFieldType() {
            super(NAME, false, false, true, TextSearchInfo.SIMPLE_MATCH_ONLY, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public boolean isSearchable() {
            // The _id field is always searchable.
            return true;
        }

        @Override
        public Query termsQuery(Collection<?> values, SearchExecutionContext context) {
            failIfNotIndexed();
            List<BytesRef> bytesRefs = values.stream().map(v -> {
                Object idObject = v;
                if (idObject instanceof BytesRef) {
                    idObject = ((BytesRef) idObject).utf8ToString();
                }
                return Uid.encodeId(idObject.toString());
            }).toList();
            return new TermInSetQuery(name(), bytesRefs);
        }

        @Override
        public Query termQuery(Object value, SearchExecutionContext context) {
            return termsQuery(Arrays.asList(value), context);
        }

        @Override
        public Query existsQuery(SearchExecutionContext context) {
            return new MatchAllDocsQuery();
        }

        @Override
        public BlockLoader blockLoader(BlockLoaderContext blContext) {
            return new BlockStoredFieldsReader.IdBlockLoader();
        }

        @Override
        public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
            return new StoredValueFetcher(context.lookup(), NAME);
        }
    }
}
