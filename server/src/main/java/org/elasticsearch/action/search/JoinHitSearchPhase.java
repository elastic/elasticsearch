/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.builder.JoinHitBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An optional fetch phase that fetch search hits from other indices associated with search hits of the main query.
 */
final class JoinHitSearchPhase extends FetchSearchPhase.ExtendedPhase {

    JoinHitSearchPhase(SearchPhaseContext context, InternalSearchResponse searchResponse,
                       AtomicArray<SearchPhaseResult> queryResults, ActionListener<Void> onFinish) {
        super("fetch_join_hits", context, searchResponse, queryResults, onFinish);
    }

    private static class JoinRequest {
        private final JoinHitBuilder query;
        private final String key;
        private final List<SearchHit> leftHits;

        public JoinRequest(JoinHitBuilder query, String key, List<SearchHit> leftHits) {
            this.query = query;
            this.key = key;
            this.leftHits = leftHits;
        }

        SearchRequest toSearchRequest() {
            final SearchSourceBuilder source = query.getSource().shallowCopy();
            source.query(new IdsQueryBuilder().addIds(key));
            return new SearchRequest(query.getIndex()).source(source);
        }
    }

    List<JoinRequest> getJoinRequests() {
        if (context.getRequest().source() == null || context.getRequest().source().joinHits() == null) {
            return Collections.emptyList();
        }

        class Group {
            final JoinHitBuilder query;
            final Map<String, List<SearchHit>> leftHits = new HashMap<>();

            Group(JoinHitBuilder query) {
                this.query = query;
            }
        }

        final List<Group> groups = context.getRequest().source().joinHits()
            .stream()
            .map(Group::new)
            .collect(Collectors.toList());

        for (SearchHit hit : searchResponse.hits) {
            for (Group group : groups) {
                final DocumentField matchField = hit.field(group.query.getMatchField());
                if (matchField == null || matchField.getValues() == null) {
                    continue;
                }
                for (Object matchValue : matchField.getValues()) {
                    group.leftHits.computeIfAbsent(matchValue.toString(), k -> new ArrayList<>()).add(hit);
                }
            }
        }
        return groups.stream()
            .flatMap(g -> g.leftHits.entrySet().stream().map(e -> new JoinRequest(g.query, e.getKey(), e.getValue())))
            .collect(Collectors.toList());
    }

    @Override
    public void run() throws IOException {
        final List<JoinRequest> joinRequests = getJoinRequests();
        if (joinRequests.isEmpty()) {
            onFinish.onResponse(null);
            return;
        }
        final MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (JoinRequest joinRequest : joinRequests) {
            multiSearchRequest.add(joinRequest.toSearchRequest());
        }
        // TODO: Using MultiSearch is not ideal as we can process and release each individual response sooner
        // Also, should we have an action that perform query+fetch in a single request?
        context.getSearchTransport().sendExecuteMultiSearch(multiSearchRequest, context.getTask(), ActionListener.wrap(mResponses -> {
            mergeJoinHits(joinRequests, mResponses.getResponses());
            onFinish.onResponse(null);
        }, onFinish::onFailure));
    }

    void mergeJoinHits(List<JoinRequest> joinRequests, MultiSearchResponse.Item[] rightResponses) {
        assert joinRequests.size() == rightResponses.length : joinRequests.size() + " != " + rightResponses.length;
        Exception failure = null;
        for (int i = 0; i < joinRequests.size(); i++) {
            final MultiSearchResponse.Item rightResponse = rightResponses[i];
            if (rightResponse.getFailure() != null) {
                failure = ExceptionsHelper.useOrSuppress(failure, rightResponse.getFailure());
            } else if (failure == null) {
                final JoinRequest joinRequest = joinRequests.get(i);
                for (SearchHit leftHit : joinRequest.leftHits) {
                    for (SearchHit rightHit : rightResponse.getResponse().getHits()) {
                        leftHit.addJoinHit(joinRequest.query.getName(), rightHit);
                    }
                }
            }
        }
        ExceptionsHelper.reThrowIfNotNull(failure);
    }
}
