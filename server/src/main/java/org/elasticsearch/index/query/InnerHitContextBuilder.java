/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchDocValuesContext;
import org.elasticsearch.search.fetch.subphase.FetchFieldsContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A builder for {@link InnerHitsContext.InnerHitSubContext}
 */
public abstract class InnerHitContextBuilder {
    protected QueryBuilder query;
    protected final InnerHitBuilder innerHitBuilder;
    protected final Map<String, InnerHitContextBuilder> children;

    protected InnerHitContextBuilder(QueryBuilder query, InnerHitBuilder innerHitBuilder, Map<String, InnerHitContextBuilder> children) {
        this.innerHitBuilder = innerHitBuilder;
        this.children = children;
        this.query = query;
    }

    public final void build(SearchContext parentSearchContext, InnerHitsContext innerHitsContext) throws IOException {
        long innerResultWindow = innerHitBuilder.getFrom() + innerHitBuilder.getSize();
        int maxInnerResultWindow = parentSearchContext.getSearchExecutionContext().getIndexSettings().getMaxInnerResultWindow();
        if (innerResultWindow > maxInnerResultWindow) {
            throw new IllegalArgumentException(
                "Inner result window is too large, the inner hit definition's ["
                    + innerHitBuilder.getName()
                    + "]'s from + size must be less than or equal to: ["
                    + maxInnerResultWindow
                    + "] but was ["
                    + innerResultWindow
                    + "]. This limit can be set by changing the ["
                    + IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey()
                    + "] index level setting."
            );
        }
        doBuild(parentSearchContext, innerHitsContext);
    }

    public InnerHitBuilder innerHitBuilder() {
        return innerHitBuilder;
    }

    protected abstract void doBuild(SearchContext parentSearchContext, InnerHitsContext innerHitsContext) throws IOException;

    public static void extractInnerHits(QueryBuilder query, Map<String, InnerHitContextBuilder> innerHitBuilders) {
        if (query instanceof AbstractQueryBuilder) {
            ((AbstractQueryBuilder<?>) query).extractInnerHitBuilders(innerHitBuilders);
        } else if (query instanceof InterceptedQueryBuilderWrapper interceptedQuery) {
            // Unwrap an intercepted query here
            extractInnerHits(interceptedQuery.queryBuilder, innerHitBuilders);
        } else {
            throw new IllegalStateException(
                "provided query builder [" + query.getClass() + "] class should inherit from AbstractQueryBuilder, but it doesn't"
            );
        }
    }

    protected void setupInnerHitsContext(
        SearchExecutionContext searchExecutionContext,
        InnerHitsContext.InnerHitSubContext innerHitsContext
    ) throws IOException {
        innerHitsContext.from(innerHitBuilder.getFrom());
        innerHitsContext.size(innerHitBuilder.getSize());
        innerHitsContext.explain(innerHitBuilder.isExplain());
        innerHitsContext.version(innerHitBuilder.isVersion());
        innerHitsContext.seqNoAndPrimaryTerm(innerHitBuilder.isSeqNoAndPrimaryTerm());
        innerHitsContext.trackScores(innerHitBuilder.isTrackScores());
        if (innerHitBuilder.getStoredFieldsContext() != null) {
            innerHitsContext.storedFieldsContext(innerHitBuilder.getStoredFieldsContext());
        }
        if (innerHitBuilder.getDocValueFields() != null) {
            FetchDocValuesContext docValuesContext = new FetchDocValuesContext(searchExecutionContext, innerHitBuilder.getDocValueFields());
            innerHitsContext.docValuesContext(docValuesContext);
        }
        if (innerHitBuilder.getFetchFields() != null) {
            FetchFieldsContext fieldsContext = new FetchFieldsContext(innerHitBuilder.getFetchFields());
            innerHitsContext.fetchFieldsContext(fieldsContext);
        }
        if (innerHitBuilder.getScriptFields() != null) {
            for (SearchSourceBuilder.ScriptField field : innerHitBuilder.getScriptFields()) {
                SearchExecutionContext innerContext = innerHitsContext.getSearchExecutionContext();
                FieldScript.Factory factory = innerContext.compile(field.script(), FieldScript.CONTEXT);
                FieldScript.LeafFactory fieldScript = factory.newFactory(field.script().getParams(), innerContext.lookup());
                innerHitsContext.scriptFields()
                    .add(
                        new org.elasticsearch.search.fetch.subphase.ScriptFieldsContext.ScriptField(
                            field.fieldName(),
                            fieldScript,
                            field.ignoreFailure()
                        )
                    );
            }
        }
        if (innerHitBuilder.getFetchSourceContext() != null) {
            innerHitsContext.fetchSourceContext(innerHitBuilder.getFetchSourceContext());
        }
        if (innerHitBuilder.getSorts() != null) {
            Optional<SortAndFormats> optionalSort = SortBuilder.buildSort(innerHitBuilder.getSorts(), searchExecutionContext);
            if (optionalSort.isPresent()) {
                innerHitsContext.sort(optionalSort.get());
            }
        }
        if (innerHitBuilder.getHighlightBuilder() != null) {
            innerHitsContext.highlight(innerHitBuilder.getHighlightBuilder().build(searchExecutionContext));
        }
        ParsedQuery parsedQuery = new ParsedQuery(
            innerHitsContext.userQueryBuilder().toQuery(searchExecutionContext),
            searchExecutionContext.copyNamedQueries()
        );
        innerHitsContext.parsedQuery(parsedQuery);
        Map<String, InnerHitsContext.InnerHitSubContext> baseChildren = buildChildInnerHits(
            innerHitsContext.parentSearchContext(),
            children
        );
        innerHitsContext.setChildInnerHits(baseChildren);
    }

    private static Map<String, InnerHitsContext.InnerHitSubContext> buildChildInnerHits(
        SearchContext parentSearchContext,
        Map<String, InnerHitContextBuilder> children
    ) throws IOException {

        Map<String, InnerHitsContext.InnerHitSubContext> childrenInnerHits = new HashMap<>();
        for (Map.Entry<String, InnerHitContextBuilder> entry : children.entrySet()) {
            InnerHitsContext childInnerHitsContext = new InnerHitsContext();
            entry.getValue().build(parentSearchContext, childInnerHitsContext);
            if (childInnerHitsContext.getInnerHits() != null) {
                childrenInnerHits.putAll(childInnerHitsContext.getInnerHits());
            }
        }
        return childrenInnerHits;
    }
}
