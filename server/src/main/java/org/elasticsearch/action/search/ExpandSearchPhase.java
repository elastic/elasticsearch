/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * This search phase is an optional phase that will be executed once all hits are fetched from the shards that executes
 * field-collapsing on the inner hits. This phase only executes if field collapsing is requested in the search request and otherwise
 * forwards to the next phase immediately.
 */
final class ExpandSearchPhase extends SearchPhase {
    private final SearchPhaseContext context;
    private final SearchHits searchHits;
    private final Supplier<SearchPhase> nextPhase;

    ExpandSearchPhase(SearchPhaseContext context, SearchHits searchHits, Supplier<SearchPhase> nextPhase) {
        super("expand");
        this.context = context;
        this.searchHits = searchHits;
        this.nextPhase = nextPhase;
    }

    /**
     * Returns <code>true</code> iff the search request has inner hits and needs field collapsing
     */
    private boolean isCollapseRequest() {
        final var searchSource = context.getRequest().source();
        return searchSource != null && searchSource.collapse() != null && searchSource.collapse().getInnerHits().isEmpty() == false;
    }

    @Override
    public void run() {
        if (isCollapseRequest() == false || searchHits.getHits().length == 0) {
            onPhaseDone();
        } else {
            doRun();
        }
    }

    private void doRun() {
        SearchRequest searchRequest = context.getRequest();
        CollapseBuilder collapseBuilder = searchRequest.source().collapse();
        final List<InnerHitBuilder> innerHitBuilders = collapseBuilder.getInnerHits();
        MultiSearchRequest multiRequest = new MultiSearchRequest();
        if (collapseBuilder.getMaxConcurrentGroupRequests() > 0) {
            multiRequest.maxConcurrentSearchRequests(collapseBuilder.getMaxConcurrentGroupRequests());
        }
        for (SearchHit hit : searchHits.getHits()) {
            BoolQueryBuilder groupQuery = new BoolQueryBuilder();
            Object collapseValue = hit.field(collapseBuilder.getField()).getValue();
            if (collapseValue != null) {
                groupQuery.filter(QueryBuilders.matchQuery(collapseBuilder.getField(), collapseValue));
            } else {
                groupQuery.mustNot(QueryBuilders.existsQuery(collapseBuilder.getField()));
            }
            QueryBuilder origQuery = searchRequest.source().query();
            if (origQuery != null) {
                groupQuery.must(origQuery);
            }
            for (InnerHitBuilder innerHitBuilder : innerHitBuilders) {
                CollapseBuilder innerCollapseBuilder = innerHitBuilder.getInnerCollapseBuilder();
                SearchSourceBuilder sourceBuilder = buildExpandSearchSourceBuilder(innerHitBuilder, innerCollapseBuilder).query(groupQuery)
                    .postFilter(searchRequest.source().postFilter())
                    .runtimeMappings(searchRequest.source().runtimeMappings());
                SearchRequest groupRequest = new SearchRequest(searchRequest);
                groupRequest.source(sourceBuilder);
                multiRequest.add(groupRequest);
            }
        }
        context.getSearchTransport().sendExecuteMultiSearch(multiRequest, context.getTask(), ActionListener.wrap(response -> {
            Iterator<MultiSearchResponse.Item> it = response.iterator();
            for (SearchHit hit : searchHits.getHits()) {
                for (InnerHitBuilder innerHitBuilder : innerHitBuilders) {
                    MultiSearchResponse.Item item = it.next();
                    if (item.isFailure()) {
                        context.onPhaseFailure(this, "failed to expand hits", item.getFailure());
                        return;
                    }
                    SearchHits innerHits = item.getResponse().getHits();
                    if (hit.getInnerHits() == null) {
                        hit.setInnerHits(Maps.newMapWithExpectedSize(innerHitBuilders.size()));
                    }
                    hit.getInnerHits().put(innerHitBuilder.getName(), innerHits);
                    innerHits.mustIncRef();
                }
            }
            onPhaseDone();
        }, context::onFailure));
    }

    private static SearchSourceBuilder buildExpandSearchSourceBuilder(InnerHitBuilder options, CollapseBuilder innerCollapseBuilder) {
        SearchSourceBuilder groupSource = new SearchSourceBuilder();
        groupSource.from(options.getFrom());
        groupSource.size(options.getSize());
        if (options.getSorts() != null) {
            options.getSorts().forEach(groupSource::sort);
        }
        if (options.getFetchSourceContext() != null) {
            if (options.getFetchSourceContext().includes().length == 0 && options.getFetchSourceContext().excludes().length == 0) {
                groupSource.fetchSource(options.getFetchSourceContext().fetchSource());
            } else {
                groupSource.fetchSource(options.getFetchSourceContext().includes(), options.getFetchSourceContext().excludes());
            }
        }
        if (options.getFetchFields() != null) {
            options.getFetchFields().forEach(groupSource::fetchField);
        }
        if (options.getDocValueFields() != null) {
            options.getDocValueFields().forEach(ff -> groupSource.docValueField(ff.field, ff.format));
        }
        if (options.getStoredFieldsContext() != null && options.getStoredFieldsContext().fieldNames() != null) {
            options.getStoredFieldsContext().fieldNames().forEach(groupSource::storedField);
        }
        if (options.getScriptFields() != null) {
            for (SearchSourceBuilder.ScriptField field : options.getScriptFields()) {
                groupSource.scriptField(field.fieldName(), field.script());
            }
        }
        if (options.getHighlightBuilder() != null) {
            groupSource.highlighter(options.getHighlightBuilder());
        }
        groupSource.explain(options.isExplain());
        groupSource.trackScores(options.isTrackScores());
        groupSource.version(options.isVersion());
        groupSource.seqNoAndPrimaryTerm(options.isSeqNoAndPrimaryTerm());
        if (innerCollapseBuilder != null) {
            groupSource.collapse(innerCollapseBuilder);
        }
        return groupSource;
    }

    private void onPhaseDone() {
        context.executeNextPhase(this, nextPhase.get());
    }
}
