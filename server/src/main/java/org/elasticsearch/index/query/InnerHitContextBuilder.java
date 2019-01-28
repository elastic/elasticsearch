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
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
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
    protected final QueryBuilder query;
    protected final InnerHitBuilder innerHitBuilder;
    protected final Map<String, InnerHitContextBuilder> children;

    protected InnerHitContextBuilder(QueryBuilder query, InnerHitBuilder innerHitBuilder, Map<String, InnerHitContextBuilder> children) {
        this.innerHitBuilder = innerHitBuilder;
        this.children = children;
        this.query = query;
    }

    public final void build(SearchContext parentSearchContext, InnerHitsContext innerHitsContext) throws IOException {
        long innerResultWindow = innerHitBuilder.getFrom() + innerHitBuilder.getSize();
        int maxInnerResultWindow = parentSearchContext.mapperService().getIndexSettings().getMaxInnerResultWindow();
        if (innerResultWindow > maxInnerResultWindow) {
            throw new IllegalArgumentException(
                "Inner result window is too large, the inner hit definition's [" + innerHitBuilder.getName() +
                    "]'s from + size must be less than or equal to: [" + maxInnerResultWindow + "] but was [" + innerResultWindow +
                    "]. This limit can be set by changing the [" + IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING.getKey() +
                    "] index level setting."
            );
        }
        doBuild(parentSearchContext, innerHitsContext);
    }

    protected abstract void doBuild(SearchContext parentSearchContext, InnerHitsContext innerHitsContext) throws IOException;

    public static void extractInnerHits(QueryBuilder query, Map<String, InnerHitContextBuilder> innerHitBuilders) {
        if (query instanceof AbstractQueryBuilder) {
            ((AbstractQueryBuilder) query).extractInnerHitBuilders(innerHitBuilders);
        } else {
            throw new IllegalStateException("provided query builder [" + query.getClass() +
                "] class should inherit from AbstractQueryBuilder, but it doesn't");
        }
    }

    protected void setupInnerHitsContext(QueryShardContext queryShardContext,
                                         InnerHitsContext.InnerHitSubContext innerHitsContext) throws IOException {
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
            innerHitsContext.docValueFieldsContext(new DocValueFieldsContext(innerHitBuilder.getDocValueFields()));
        }
        if (innerHitBuilder.getScriptFields() != null) {
            for (SearchSourceBuilder.ScriptField field : innerHitBuilder.getScriptFields()) {
                QueryShardContext innerContext = innerHitsContext.getQueryShardContext();
                FieldScript.Factory factory = innerContext.getScriptService().compile(field.script(), FieldScript.CONTEXT);
                FieldScript.LeafFactory fieldScript = factory.newFactory(field.script().getParams(), innerHitsContext.lookup());
                innerHitsContext.scriptFields().add(new org.elasticsearch.search.fetch.subphase.ScriptFieldsContext.ScriptField(
                    field.fieldName(), fieldScript, field.ignoreFailure()));
            }
        }
        if (innerHitBuilder.getFetchSourceContext() != null) {
            innerHitsContext.fetchSourceContext(innerHitBuilder.getFetchSourceContext() );
        }
        if (innerHitBuilder.getSorts() != null) {
            Optional<SortAndFormats> optionalSort = SortBuilder.buildSort(innerHitBuilder.getSorts(), queryShardContext);
            if (optionalSort.isPresent()) {
                innerHitsContext.sort(optionalSort.get());
            }
        }
        if (innerHitBuilder.getHighlightBuilder() != null) {
            innerHitsContext.highlight(innerHitBuilder.getHighlightBuilder().build(queryShardContext));
        }
        ParsedQuery parsedQuery = new ParsedQuery(query.toQuery(queryShardContext), queryShardContext.copyNamedQueries());
        innerHitsContext.parsedQuery(parsedQuery);
        Map<String, InnerHitsContext.InnerHitSubContext> baseChildren =
            buildChildInnerHits(innerHitsContext.parentSearchContext(), children);
        innerHitsContext.setChildInnerHits(baseChildren);
    }

    private static Map<String, InnerHitsContext.InnerHitSubContext> buildChildInnerHits(SearchContext parentSearchContext,
                                Map<String, InnerHitContextBuilder> children) throws IOException {

        Map<String, InnerHitsContext.InnerHitSubContext> childrenInnerHits = new HashMap<>();
        for (Map.Entry<String, InnerHitContextBuilder> entry : children.entrySet()) {
            InnerHitsContext childInnerHitsContext = new InnerHitsContext();
            entry.getValue().build(
                parentSearchContext, childInnerHitsContext);
            if (childInnerHitsContext.getInnerHits() != null) {
                childrenInnerHits.putAll(childInnerHitsContext.getInnerHits());
            }
        }
        return childrenInnerHits;
    }
}
