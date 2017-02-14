/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.function.Function;

/**
 * This search phase is an optional phase that will be executed once all hits are fetched from the shards that executes
 * field-collapsing on the inner hits. This phase only executes if field collapsing is requested in the search request and otherwise
 * forwards to the next phase immediately.
 */
final class ExpandSearchPhase extends SearchPhase {
    private final SearchPhaseContext context;
    private final SearchResponse searchResponse;
    private final Function<SearchResponse, SearchPhase> nextPhaseFactory;

    ExpandSearchPhase(SearchPhaseContext context, SearchResponse searchResponse,
                      Function<SearchResponse, SearchPhase> nextPhaseFactory) {
        super("expand");
        this.context = context;
        this.searchResponse = searchResponse;
        this.nextPhaseFactory = nextPhaseFactory;
    }

    /**
     * Returns <code>true</code> iff the search request has inner hits and needs field collapsing
     */
    private boolean isCollapseRequest() {
        final SearchRequest searchRequest = context.getRequest();
        return searchRequest.source() != null &&
            searchRequest.source().collapse() != null &&
            searchRequest.source().collapse().getInnerHit() != null;
    }

    @Override
    public void run() throws IOException {
        if (isCollapseRequest()) {
            SearchRequest searchRequest = context.getRequest();
            CollapseBuilder collapseBuilder = searchRequest.source().collapse();
            MultiSearchRequest multiRequest = new MultiSearchRequest();
            if (collapseBuilder.getMaxConcurrentGroupRequests() > 0) {
                multiRequest.maxConcurrentSearchRequests(collapseBuilder.getMaxConcurrentGroupRequests());
            }
            for (SearchHit hit : searchResponse.getHits()) {
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
                SearchSourceBuilder sourceBuilder = buildExpandSearchSourceBuilder(collapseBuilder.getInnerHit())
                    .query(groupQuery);
                SearchRequest groupRequest = new SearchRequest(searchRequest.indices())
                    .types(searchRequest.types())
                    .source(sourceBuilder);
                multiRequest.add(groupRequest);
            }
            context.getSearchTransport().sendExecuteMultiSearch(multiRequest, context.getTask(),
                ActionListener.wrap(response -> {
                    Iterator<MultiSearchResponse.Item> it = response.iterator();
                    for (SearchHit hit : searchResponse.getHits()) {
                        MultiSearchResponse.Item item = it.next();
                        if (item.isFailure()) {
                            context.onPhaseFailure(this, "failed to expand hits", item.getFailure());
                            return;
                        }
                        SearchHits innerHits = item.getResponse().getHits();
                        if (hit.getInnerHits() == null) {
                            hit.setInnerHits(new HashMap<>(1));
                        }
                        hit.getInnerHits().put(collapseBuilder.getInnerHit().getName(), innerHits);
                    }
                    context.executeNextPhase(this, nextPhaseFactory.apply(searchResponse));
                }, context::onFailure)
            );
        } else {
            context.executeNextPhase(this, nextPhaseFactory.apply(searchResponse));
        }
    }

    private SearchSourceBuilder buildExpandSearchSourceBuilder(InnerHitBuilder options) {
        SearchSourceBuilder groupSource = new SearchSourceBuilder();
        groupSource.from(options.getFrom());
        groupSource.size(options.getSize());
        if (options.getSorts() != null) {
            options.getSorts().forEach(groupSource::sort);
        }
        if (options.getFetchSourceContext() != null) {
            if (options.getFetchSourceContext().includes() == null && options.getFetchSourceContext().excludes() == null) {
                groupSource.fetchSource(options.getFetchSourceContext().fetchSource());
            } else {
                groupSource.fetchSource(options.getFetchSourceContext().includes(),
                    options.getFetchSourceContext().excludes());
            }
        }
        if (options.getDocValueFields() != null) {
            options.getDocValueFields().forEach(groupSource::docValueField);
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
        return groupSource;
    }
}
