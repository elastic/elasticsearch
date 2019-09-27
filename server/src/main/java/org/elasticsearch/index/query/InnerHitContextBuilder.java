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

package org.elasticsearch.index.query;

import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * A builder for {@link InnerHitsContext.InnerHitsSubContext}
 */
public abstract class InnerHitContextBuilder {
    // By default return 3 hits per bucket. A higher default would make the response really large by default, since
    // the to hits are returned per bucket.
    private static final int DEFAULT_SIZE = 3;

    protected final QueryBuilder query;
    protected final InnerHitBuilder innerHitBuilder;
    protected final Map<String, InnerHitContextBuilder> children;

    protected InnerHitContextBuilder(QueryBuilder query, InnerHitBuilder innerHitBuilder, Map<String, InnerHitContextBuilder> children) {
        this.innerHitBuilder = innerHitBuilder;
        this.children = children;
        this.query = query;
    }

    public final void validate(QueryShardContext queryShardContext) {
        long innerResultWindow = innerHitBuilder.getFrom() + innerHitBuilder.getSize();
        int maxInnerResultWindow = queryShardContext.getIndexSettings().getMaxInnerResultWindow();
        if (innerResultWindow > maxInnerResultWindow) {
            throw new IllegalArgumentException(
                "Inner result window is too large, the inner hit definition's [" + innerHitBuilder.getName() +
                    "]'s from + size must be less than or equal to: [" + maxInnerResultWindow + "] but was [" + innerResultWindow +
                    "]. This limit can be set by changing the [" + IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey() +
                    "] index level setting."
            );
        }
        doValidate(queryShardContext);
    }

    public InnerHitBuilder innerHitBuilder() {
        return innerHitBuilder;
    }

    protected abstract void doValidate(QueryShardContext queryShardContext);

    public abstract void build(SearchContext parentSearchContext, InnerHitsContext innerHitsContext) throws IOException;

    public static void extractInnerHits(QueryBuilder query, Map<String, InnerHitContextBuilder> innerHitBuilders) {
        if (query instanceof AbstractQueryBuilder) {
            ((AbstractQueryBuilder) query).extractInnerHitBuilders(innerHitBuilders);
        } else {
            throw new IllegalStateException("provided query builder [" + query.getClass() +
                "] class should inherit from AbstractQueryBuilder, but it doesn't");
        }
    }

    protected SearchContext createSubSearchContext(QueryShardContext cloneShardContext, SearchContext parentContext) throws IOException {
        SearchContext.Builder builder = new SearchContext.Builder(parentContext.id(),
            parentContext.getTask(),
            parentContext.nodeId(),
            parentContext.indexShard(),
            cloneShardContext,
            parentContext.searcher(),
            parentContext.fetchPhase(),
            parentContext.shardTarget().getClusterAlias(),
            parentContext.numberOfShards(),
            parentContext::getRelativeTimeInMillis,
            parentContext.source());
        if (innerHitBuilder.getFrom() != -1) {
            builder.setFrom(innerHitBuilder.getFrom());
        }
        if (innerHitBuilder.getSize() != -1) {
            builder.setSize(innerHitBuilder.getSize());
        } else {
            builder.setSize(DEFAULT_SIZE);
        }
        builder.setExplain(innerHitBuilder.isExplain());
        builder.setVersion(innerHitBuilder.isVersion());
        builder.setSeqAndPrimaryTerm(innerHitBuilder.isSeqNoAndPrimaryTerm());
        builder.setTrackScores(innerHitBuilder.isTrackScores());
        if (innerHitBuilder.getStoredFieldsContext() != null) {
            builder.setStoredFields(innerHitBuilder.getStoredFieldsContext());
        }
        if (innerHitBuilder.getDocValueFields() != null) {
            builder.setDocValueFields(new DocValueFieldsContext(innerHitBuilder.getDocValueFields()));
        }
        if (innerHitBuilder.getScriptFields() != null && innerHitBuilder.getSize() != 0) {
            builder.buildScriptFields(cloneShardContext.getScriptService(), innerHitBuilder.getScriptFields());
        }
        if (innerHitBuilder.getFetchSourceContext() != null) {
            builder.setFetchSource(innerHitBuilder.getFetchSourceContext());
        }
        if (innerHitBuilder.getSorts() != null) {
            Optional<SortAndFormats> optionalSort = SortBuilder.buildSort(innerHitBuilder.getSorts(), cloneShardContext);
            if (optionalSort.isPresent()) {
                builder.setSort(optionalSort.get());
            }
        }
        if (innerHitBuilder.getHighlightBuilder() != null) {
            builder.buildHighlight(innerHitBuilder.getHighlightBuilder());
        }
        builder.setInnerHits(children);
        builder.setQuery(new ParsedQuery(query.toQuery(cloneShardContext), cloneShardContext.copyNamedQueries()));
        return builder.build(() -> {});
    }
}
