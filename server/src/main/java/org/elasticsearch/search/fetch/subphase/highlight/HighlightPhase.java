/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.PerDocumentQueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class HighlightPhase implements FetchSubPhase {

    private final Map<String, Highlighter> highlighters;

    public HighlightPhase(Map<String, Highlighter> highlighters) {
        this.highlighters = highlighters;
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        if (context.highlight() == null) {
            return null;
        }
        // We apply the per-document rewrite context to prevent costly nearest neighbor searches
        // during query rewriting. Some highlighters (unified, semantic) execute the provided query on a document
        // to extract a score or determine if it matched a specific clause.
        // Running a full nearest neighbor search in this context would be inefficient,
        // so we rely on this rewriting mechanism to avoid unnecessary computation.
        PerDocumentQueryRewriteContext rewriteContext = new PerDocumentQueryRewriteContext(
            context.getSearchExecutionContext().getParserConfig(),
            context.getSearchExecutionContext()::nowInMillis
        );
        try {
            var query = Rewriteable.rewrite(context.userQueryBuilder(), rewriteContext, true).toQuery(context.getSearchExecutionContext());
            return getProcessor(context, context.highlight(), query);
        } catch (IOException e) {
            throw new UncheckedIOException("Exception during the rewrite of the highlight query", e);
        }
    }

    public FetchSubPhaseProcessor getProcessor(FetchContext context, SearchHighlightContext highlightContext, Query query) {
        Map<String, Object> sharedCache = new HashMap<>();
        FieldContext fieldContext = contextBuilders(context, highlightContext, query, sharedCache);

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {

            }

            @Override
            public StoredFieldsSpec storedFieldsSpec() {
                return fieldContext.storedFieldsSpec;
            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                Map<String, HighlightField> highlightFields = new HashMap<>();
                Map<String, Function<HitContext, FieldHighlightContext>> contextBuilders = fieldContext.builders;
                for (String field : contextBuilders.keySet()) {
                    FieldHighlightContext fieldContext = contextBuilders.get(field).apply(hitContext);
                    Highlighter highlighter = getHighlighter(fieldContext.field, fieldContext.fieldType);
                    HighlightField highlightField = highlighter.highlight(fieldContext);
                    if (highlightField != null) {
                        // Note that we make sure to use the original field name in the response. This is because the
                        // original field could be an alias, and highlighter implementations may instead reference the
                        // concrete field it points to.
                        highlightFields.put(field, new HighlightField(field, highlightField.fragments()));
                    }
                }
                hitContext.hit().highlightFields(highlightFields);
            }
        };
    }

    private Highlighter getHighlighter(SearchHighlightContext.Field field, MappedFieldType fieldType) {
        String highlighterType = field.fieldOptions().highlighterType();
        if (highlighterType == null) {
            highlighterType = fieldType.getDefaultHighlighter();
        }
        Highlighter highlighter = highlighters.get(highlighterType);
        if (highlighter == null) {
            throw new IllegalArgumentException("unknown highlighter type [" + highlighterType + "] for the field [" + field.field() + "]");
        }
        return highlighter;
    }

    private record FieldContext(StoredFieldsSpec storedFieldsSpec, Map<String, Function<HitContext, FieldHighlightContext>> builders) {}

    private FieldContext contextBuilders(
        FetchContext context,
        SearchHighlightContext highlightContext,
        Query query,
        Map<String, Object> sharedCache
    ) {
        Map<String, Function<HitContext, FieldHighlightContext>> builders = new LinkedHashMap<>();
        StoredFieldsSpec storedFieldsSpec = StoredFieldsSpec.NO_REQUIREMENTS;
        for (SearchHighlightContext.Field field : highlightContext.fields()) {
            Collection<String> fieldNamesToHighlight = context.getSearchExecutionContext().getMatchingFieldNames(field.field());

            boolean fieldNameContainsWildcards = field.field().contains("*");
            Set<String> storedFields = new HashSet<>();
            boolean sourceRequired = false;
            for (String fieldName : fieldNamesToHighlight) {
                MappedFieldType fieldType = context.getSearchExecutionContext().getFieldType(fieldName);
                Highlighter highlighter = getHighlighter(field, fieldType);

                // We should prevent highlighting if a field is anything but a text, match_only_text,
                // or keyword field.
                // However, someone might implement a custom field type that has text and still want to
                // highlight on that. We cannot know in advance if the highlighter will be able to
                // highlight such a field and so we do the following:
                // If the field is only highlighted because the field matches a wildcard we assume
                // it was a mistake and do not process it.
                // If the field was explicitly given we assume that whoever issued the query knew
                // what they were doing and try to highlight anyway.
                if (fieldNameContainsWildcards) {
                    if (fieldType.typeName().equals(TextFieldMapper.CONTENT_TYPE) == false
                        && fieldType.typeName().equals(KeywordFieldMapper.CONTENT_TYPE) == false
                        && fieldType.typeName().equals("match_only_text") == false) {
                        continue;
                    }
                    if (highlighter.canHighlight(fieldType) == false) {
                        continue;
                    }
                }

                if (fieldType.isStored()) {
                    storedFields.add(fieldType.name());
                } else {
                    sourceRequired = true;
                }

                Query highlightQuery = field.fieldOptions().highlightQuery();

                builders.put(
                    fieldName,
                    hc -> new FieldHighlightContext(
                        fieldType.name(),
                        field,
                        fieldType,
                        context,
                        hc,
                        highlightQuery == null ? query : highlightQuery,
                        sharedCache
                    )
                );
            }
            storedFieldsSpec = storedFieldsSpec.merge(new StoredFieldsSpec(sourceRequired, false, storedFields));
        }
        return new FieldContext(storedFieldsSpec, builders);
    }
}
