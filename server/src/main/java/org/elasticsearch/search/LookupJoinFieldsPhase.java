/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.builder.JoinField;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Asynchronously fetch fields from other indices specified in {@link  SearchSourceBuilder#joinFields()}
 */
public final class LookupJoinFieldsPhase {
    private static final String FIELD_FAILURE = "_failure";

    private final TransportService transportService;

    public LookupJoinFieldsPhase(TransportService transportService) {
        this.transportService = transportService;
    }

    private static class LookupRequest {
        final int fieldOrd;
        final JoinField joinField;
        final String id;
        final List<SearchHit> leftHits = new ArrayList<>();

        LookupRequest(int fieldOrd, JoinField joinField, String id) {
            this.fieldOrd = fieldOrd;
            this.joinField = joinField;
            this.id = id;
        }

        void addHit(SearchHit leftHit) {
            leftHits.add(leftHit);
        }
    }

    private static List<LookupRequest> getLookupRequests(SearchHits leftHits, List<JoinField> joinFields) {
        final List<LookupRequest> lookupRequests = new ArrayList<>();
        for (int fieldOrd = 0; fieldOrd < joinFields.size(); fieldOrd++) {
            final Map<String, SearchHit.JoinHit> joinHits = new HashMap<>();
            for (SearchHit leftHit : leftHits.getHits()) {
                for (SearchHit.JoinHit joinHit : leftHit.getJoinHits(fieldOrd)) {
                    joinHits.put(joinHit.getId(), joinHit);
                }
            }
            final JoinField joinField = joinFields.get(fieldOrd);
            final Map<String, LookupRequest> toLookup = new HashMap<>(); // id -> request
            for (SearchHit leftHit : leftHits.getHits()) {
                final DocumentField idField = leftHit.field(joinField.getKeyField());
                if (idField == null) {
                    continue;
                }
                final Set<String> fetchedIds = leftHit.getJoinHits(fieldOrd)
                    .stream()
                    .map(SearchHit.JoinHit::getId)
                    .collect(Collectors.toSet());
                final Set<String> ids = idField.getValues().stream().map(Objects::toString).collect(Collectors.toSet());
                for (String id : ids) {
                    if (fetchedIds.contains(id)) {
                        continue;
                    }
                    final SearchHit.JoinHit joinHit = joinHits.get(id);
                    if (joinHit != null) {
                        leftHit.addJoinHit(fieldOrd, joinHit);
                    } else {
                        final int finalOrd = fieldOrd;
                        toLookup.computeIfAbsent(id, k -> new LookupRequest(finalOrd, joinField, id)).addHit(leftHit);
                    }
                }
            }
            lookupRequests.addAll(toLookup.values());
        }
        return lookupRequests;
    }

    /**
     * Asynchronously look up join fields specified in a search request
     *
     * @param clusterAlias       the cluster alias of the search request - join fields are resolved only in the local cluster
     * @param parentTask         the parent task of the search request
     * @param leftHits           the left hits that will be enriched with join fields
     * @param searchRequest      the source of the search request
     * @param onlyLocalNode      if {@code true} then the lookup will use only the local node
     * @param recordFailures     if {@code true} then failures will be recorded in the result
     * @param mergeToFetchFields if {@code true} then the lookup results will be merged into the regular fetch fields;
     *                           otherwise, the result will be stored in the temporary fields of the corresponding hits.
     * @param origListener       the listener to be called when the lookup completes
     */
    public void lookupJoinFields(
        String clusterAlias,
        Task parentTask,
        SearchHits leftHits,
        SearchSourceBuilder searchRequest,
        boolean onlyLocalNode,
        boolean recordFailures,
        boolean mergeToFetchFields,
        ActionListener<Void> origListener
    ) {
        if (clusterAlias != null && RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias) == false) {
            origListener.onResponse(null);
            return;
        }
        if (searchRequest == null || searchRequest.joinFields() == null || leftHits == null) {
            origListener.onResponse(null);
            return;
        }
        final ActionListener<Void> listener = ActionListener.wrap(() -> {
            if (mergeToFetchFields) {
                leftHits.forEach(SearchHit::mergeJoinHitsToFields);
            }
            origListener.onResponse(null);
        });

        final List<LookupRequest> lookupRequests = getLookupRequests(leftHits, searchRequest.joinFields());
        if (lookupRequests.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        final MultiGetRequest multiGetRequest = new MultiGetRequest().realtime(true);
        if (onlyLocalNode) {
            multiGetRequest.preference(Preference.ONLY_LOCAL.type());
        }
        for (LookupRequest lookupRequest : lookupRequests) {
            final JoinField joinField = lookupRequest.joinField;
            final MultiGetRequest.Item item = new MultiGetRequest.Item(joinField.getIndex(), lookupRequest.id).fetchSourceContext(
                new FetchSourceContext(true, joinField.getFields(), Strings.EMPTY_ARRAY)
            );
            multiGetRequest.add(item);
        }

        transportService.sendChildRequest(
            transportService.getLocalNodeConnection(),
            MultiGetAction.NAME,
            multiGetRequest,
            parentTask,
            new TransportResponseHandler<MultiGetResponse>() {
                @Override
                public void handleResponse(MultiGetResponse resp) {
                    handleMultiGetResponses(lookupRequests, resp.getResponses(), recordFailures);
                    listener.onResponse(null);
                }

                @Override
                public void handleException(TransportException exp) {
                    if (recordFailures) {
                        handleMultiGetFailure(lookupRequests, ExceptionsHelper.unwrapCause(exp));
                    }
                    listener.onResponse(null);
                }

                @Override
                public MultiGetResponse read(StreamInput in) throws IOException {
                    return new MultiGetResponse(in);
                }
            }
        );
    }

    private static void handleMultiGetResponses(
        List<LookupRequest> lookupRequests,
        MultiGetItemResponse[] responseItems,
        boolean recordFailures
    ) {
        assert lookupRequests.size() == responseItems.length : lookupRequests.size() + " != " + responseItems.length;
        for (int i = 0; i < lookupRequests.size(); i++) {
            final LookupRequest lookupRequest = lookupRequests.get(i);
            final String id = lookupRequest.id;
            final Map<String, List<Object>> fields;
            if (responseItems[i].getFailure() != null) {
                if (recordFailures == false) {
                    continue;
                }
                fields = Map.of(FIELD_FAILURE, List.of(responseItems[i].getFailure().getMessage()));
            } else {
                final GetResponse response = responseItems[i].getResponse();
                if (response.isExists()) {
                    final Map<String, Object> source = response.getSourceAsMap();
                    fields = source.keySet()
                        .stream()
                        .collect(Collectors.toMap(field -> field, field -> XContentMapValues.extractRawValues(field, source)));
                } else {
                    final String index = lookupRequest.joinField.getIndex();
                    final String error = "id [" + id + "] on index [" + index + "] doesn't exist";
                    fields = Map.of(FIELD_FAILURE, List.of(error));
                }
            }
            final SearchHit.JoinHit joinHit = new SearchHit.JoinHit(lookupRequest.joinField.getName(), id, fields);
            for (SearchHit leftHit : lookupRequest.leftHits) {
                leftHit.addJoinHit(lookupRequest.fieldOrd, joinHit);
            }
        }
    }

    private static void handleMultiGetFailure(List<LookupRequest> lookupRequests, Throwable ex) {
        final Map<String, List<Object>> fields = Map.of(FIELD_FAILURE, List.of(ex.getMessage()));
        for (LookupRequest lookupRequest : lookupRequests) {
            final SearchHit.JoinHit joinHit = new SearchHit.JoinHit(lookupRequest.joinField.getName(), lookupRequest.id, fields);
            for (SearchHit leftHit : lookupRequest.leftHits) {
                leftHit.addJoinHit(lookupRequest.fieldOrd, joinHit);
            }
        }
    }
}
