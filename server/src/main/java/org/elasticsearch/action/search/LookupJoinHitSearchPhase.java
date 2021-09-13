/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.builder.JoinHitLookupBuilder;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An optional fetch phase that lookup and enriches documents from other indices associated with search hits of the main query.
 */
public final class LookupJoinHitSearchPhase extends FetchSearchPhase.ExtendedPhase {

    LookupJoinHitSearchPhase(SearchPhaseContext context, InternalSearchResponse searchResponse,
                             AtomicArray<SearchPhaseResult> queryResults, ActionListener<SearchPhase> onFinish) {
        super("lookup_join_hits", context, searchResponse, queryResults, onFinish);
    }

    @Override
    public void run() throws IOException {
        if (context.getRequest().source() == null) {
            onFinish.onResponse(null);
            return;
        }

        final List<LookupRequest> lookupRequests = getLookupRequests(
            context.getRequest().source().lookupJoinHitBuilders(), searchResponse.hits);
        final MultiGetRequest multiGetRequest = new MultiGetRequest().realtime(true);
        for (LookupRequest lookupRequest : lookupRequests) {
            multiGetRequest.add(lookupRequest.toGetRequest());
        }
        if (multiGetRequest.getItems().isEmpty()) {
            onFinish.onResponse(null);
            return;
        }
        context.getSearchTransport().sendMultiGetRequest(multiGetRequest, context.getTask(), ActionListener.wrap(mResponses -> {
            mergeJoinHits(lookupRequests, mResponses.getResponses());
            onFinish.onResponse(null);
        }, onFinish::onFailure));
    }

    static final class LookupRequest {
        private final JoinHitLookupBuilder query;
        private final String matchId;
        private final List<SearchHit> leftHits;

        LookupRequest(JoinHitLookupBuilder query, String matchId, List<SearchHit> leftHits) {
            this.query = query;
            this.matchId = matchId;
            this.leftHits = leftHits;
        }

        JoinHitLookupBuilder getQuery() {
            return query;
        }

        String getIndex() {
            return query.getIndex();
        }

        String getName() {
            return query.getName();
        }

        String getMatchId() {
            return matchId;
        }

        List<SearchHit> getLeftHits() {
            return leftHits;
        }

        MultiGetRequest.Item toGetRequest() {
            MultiGetRequest.Item item = new MultiGetRequest.Item(query.getIndex(), matchId);
            item.fetchSourceContext(query.getFetchSourceContext());
            item.storedFields(query.getStoredFields());
            return item;
        }
    }

    static List<LookupRequest> getLookupRequests(List<JoinHitLookupBuilder> specs, SearchHits leftHits) {
        if (specs.isEmpty()) {
            return Collections.emptyList();
        }
        // TODO: Maybe combine groups that lookup join hits from the same index (e.g., from_user and to_user) to the same request.
        class Group {
            final JoinHitLookupBuilder query;
            final Map<String, List<SearchHit>> leftHits = new HashMap<>();

            Group(JoinHitLookupBuilder query) {
                this.query = query;
            }
        }
        final List<Group> groups = specs.stream().map(Group::new).collect(Collectors.toList());
        for (Group group : groups) {
            final Map<String, SearchHit.JoinHit> cachedJoinHits = new HashMap<>();
            for (SearchHit leftHit : leftHits.getHits()) {
                final Map<String, List<SearchHit.JoinHit>> joinHits = leftHit.getJoinHits();
                if (joinHits != null) {
                    for (SearchHit.JoinHit joinHit : joinHits.getOrDefault(group.query.getName(), List.of())) {
                        cachedJoinHits.put(joinHit.getId(), joinHit);
                    }
                }
            }
            for (SearchHit leftHit : leftHits) {
                final List<Object> matchValues;
                // Lookup from fields first; then try from _source.
                final DocumentField matchField = leftHit.field(group.query.getMatchField());
                if (matchField != null && matchField.getValues() != null) {
                    matchValues = matchField.getValues();
                } else if (leftHit.hasSource()) {
                    matchValues = XContentMapValues.extractRawValues(group.query.getMatchField(), leftHit.getSourceAsMap());
                } else {
                    continue;
                }
                // Skip join hits that are already fetched on the data node.
                final List<SearchHit.JoinHit> fetchedJoinHits = leftHit.getJoinHits().getOrDefault(group.query.getName(), List.of());
                for (Object matchValue : matchValues) {
                    if (matchValue == null) {
                        continue;
                    }
                    final String matchId = matchValue.toString();
                    if (fetchedJoinHits.stream().anyMatch(j -> j.getId().equals(matchId))) {
                        continue;
                    }
                    final SearchHit.JoinHit cachedHit = cachedJoinHits.get(matchId);
                    if (cachedHit != null) {
                        leftHit.addJoinHit(group.query.getName(), cachedHit);
                    } else {
                        group.leftHits.computeIfAbsent(matchId, k -> new ArrayList<>()).add(leftHit);
                    }
                }
            }
        }
        return groups.stream()
            .flatMap(g -> g.leftHits.entrySet().stream().map(e -> new LookupRequest(g.query, e.getKey(), e.getValue())))
            .collect(Collectors.toList());
    }

    static void mergeJoinHits(List<LookupRequest> lookupRequests, MultiGetItemResponse[] rightResponses) {
        assert lookupRequests.size() == rightResponses.length : lookupRequests.size() + " != " + rightResponses.length;
        for (int i = 0; i < lookupRequests.size(); i++) {
            final MultiGetItemResponse rightResponse = rightResponses[i];
            final LookupRequest lookupRequest = lookupRequests.get(i);
            final SearchHit.JoinHit joinHit;
            if (rightResponse.getFailure() != null) {
                joinHit = new SearchHit.JoinHit(lookupRequest.matchId, rightResponse.getFailure().getFailure());
            } else {
                final GetResult getResult = rightResponse.getResponse().getResult();
                if (getResult.isExists()) {
                    assert Objects.equals(lookupRequest.getMatchId(), getResult.getId()) :
                        lookupRequest.getMatchId() + " != " + getResult.getId();
                    joinHit = new SearchHit.JoinHit(getResult.getId(), getResult.getDocumentFields(), getResult.internalSourceRef());
                } else {
                    joinHit = new SearchHit.JoinHit(lookupRequest.getMatchId(),
                        new ResourceNotFoundException("join hit for id [" + lookupRequest.getMatchId() + "] " +
                            "on index [" + lookupRequest.getIndex() + "] doesn't exist"));
                }
            }
            for (SearchHit leftHit : lookupRequest.getLeftHits()) {
                leftHit.addJoinHit(lookupRequest.query.getName(), joinHit);
            }
        }
    }
}
