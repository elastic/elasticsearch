/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
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

        return getProcessor(context, context.highlight(), context.parsedQuery().query());
    }

    public FetchSubPhaseProcessor getProcessor(FetchContext context, SearchHighlightContext highlightContext, Query query) {
        Map<String, Object> sharedCache = new HashMap<>();
        Map<String, Function<HitContext, FieldHighlightContext>> contextBuilders = contextBuilders(
            context,
            highlightContext,
            query,
            sharedCache
        );

        return new FetchSubPhaseProcessor() {
            @Override
            public void setNextReader(LeafReaderContext readerContext) {

            }

            @Override
            public void process(HitContext hitContext) throws IOException {
                Map<String, HighlightField> highlightFields = new HashMap<>();
                for (String field : contextBuilders.keySet()) {
                    FieldHighlightContext fieldContext = contextBuilders.get(field).apply(hitContext);
                    Highlighter highlighter = getHighlighter(fieldContext.field);
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

    private Highlighter getHighlighter(SearchHighlightContext.Field field) {
        String highlighterType = field.fieldOptions().highlighterType();
        if (highlighterType == null) {
            highlighterType = "unified";
        }
        Highlighter highlighter = highlighters.get(highlighterType);
        if (highlighter == null) {
            throw new IllegalArgumentException("unknown highlighter type [" + highlighterType + "] for the field [" + field.field() + "]");
        }
        return highlighter;
    }

    private Map<String, Function<HitContext, FieldHighlightContext>> contextBuilders(
        FetchContext context,
        SearchHighlightContext highlightContext,
        Query query,
        Map<String, Object> sharedCache
    ) {
        Map<String, Function<HitContext, FieldHighlightContext>> builders = new LinkedHashMap<>();
        for (SearchHighlightContext.Field field : highlightContext.fields()) {
            Highlighter highlighter = getHighlighter(field);
            Collection<String> fieldNamesToHighlight = context.getSearchExecutionContext().getMatchingFieldNames(field.field());

            if (highlightContext.forceSource(field)) {
                if (context.getSearchExecutionContext().isSourceEnabled() == false) {
                    throw new IllegalArgumentException("source is forced for fields " + fieldNamesToHighlight + " but _source is disabled");
                }
            }

            boolean fieldNameContainsWildcards = field.field().contains("*");
            for (String fieldName : fieldNamesToHighlight) {
                MappedFieldType fieldType = context.getSearchExecutionContext().getFieldType(fieldName);

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

                Query highlightQuery = field.fieldOptions().highlightQuery();

                boolean forceSource = highlightContext.forceSource(field);
                builders.put(
                    fieldName,
                    hc -> new FieldHighlightContext(
                        fieldType.name(),
                        field,
                        fieldType,
                        context,
                        hc,
                        highlightQuery == null ? query : highlightQuery,
                        forceSource,
                        sharedCache
                    )
                );
            }
        }
        return builders;
    }
}
