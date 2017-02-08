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

package org.elasticsearch.search.collapse;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * A search response listener that intercepts the search response and expands collapsed hits
 * using the {@link CollapseBuilder#innerHit} options.
 */
public class ExpandCollapseSearchResponseListener implements BiConsumer<SearchRequest, SearchResponse> {
    private final Client client;

    public ExpandCollapseSearchResponseListener(Client client) {
        this.client = Objects.requireNonNull(client);
    }

    @Override
    public void accept(SearchRequest searchRequest, SearchResponse searchResponse) {
        if (searchRequest.source() == null) {
            return ;
        }
        CollapseBuilder collapseBuilder = searchRequest.source().collapse();
        if (collapseBuilder == null || collapseBuilder.getInnerHit() == null) {
            return ;
        }
        for (SearchHit hit : searchResponse.getHits()) {
            SearchHit internalHit = (SearchHit) hit;
            BoolQueryBuilder groupQuery = new BoolQueryBuilder();
            Object collapseValue = internalHit.field(collapseBuilder.getField()).getValue();
            if (collapseValue != null) {
                groupQuery.filter(QueryBuilders.matchQuery(collapseBuilder.getField(), collapseValue));
            } else {
                groupQuery.mustNot(QueryBuilders.existsQuery(collapseBuilder.getField()));
            }
            QueryBuilder origQuery = searchRequest.source().query();
            if (origQuery != null) {
                groupQuery.must(origQuery);
            }
            SearchSourceBuilder sourceBuilder = createGroupSearchBuilder(collapseBuilder.getInnerHit())
                .query(groupQuery);
            SearchRequest groupRequest = new SearchRequest(searchRequest.indices())
                .types(searchRequest.types())
                .source(sourceBuilder);
            SearchResponse groupResponse = client.search(groupRequest).actionGet();
            SearchHits innerHits = groupResponse.getHits();
            if (internalHit.getInnerHits() == null) {
                internalHit.setInnerHits(new HashMap<>(1));
            }
            internalHit.getInnerHits().put(collapseBuilder.getInnerHit().getName(), innerHits);
        }
    }

    private SearchSourceBuilder createGroupSearchBuilder(InnerHitBuilder options) {
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
