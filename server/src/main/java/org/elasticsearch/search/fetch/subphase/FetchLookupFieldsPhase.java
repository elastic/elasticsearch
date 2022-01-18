/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
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
import java.util.function.BooleanSupplier;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * Asynchronously fetch {@link LookupField} that are specified {@link DocumentField#getLookupFields()}
 *
 * @see org.elasticsearch.index.mapper.LookupRuntimeFieldType
 */
public final class FetchLookupFieldsPhase {

    private final TransportService transportService;
    private final BooleanSupplier allowExpensiveQueries;

    public FetchLookupFieldsPhase(TransportService transportService, BooleanSupplier allowExpensiveQueries) {
        this.transportService = transportService;
        this.allowExpensiveQueries = allowExpensiveQueries;
    }

    private record LookupRequest(LookupKey key, List<LookupField> fields) {

    }

    private record LookupKey(String index, String id, List<String> fields) {
        LookupKey(LookupField field) {
            this(field.getLookupIndex(), field.getLookupId(), field.getLookupFields());
        }
    }

    /**
     * Asynchronously fetches the lookup fields specified in {@link DocumentField#getLookupFields()} of the given search hits.
     *
     * @param clusterAlias      the cluster alias of the search request - lookup fields are resolved in only the local cluster
     * @param parentTask        the parent task of the search request
     * @param hits              the search hits
     * @param onlyLocalNode     if {@code true} then the lookup process will use only the local node
     * @param recordFailures    if {@code true} then failures will be recorded in the result; otherwise, errors will be ignored
     *                          and the lookup fields can be resolved again
     * @param mergeLookupFields if {@code true} then resolved lookup fields will be merged as regular fields to {@link DocumentField}
     * @param listener          the listener to be called when the lookup completes
     */
    public void fetchLookupFields(
        String clusterAlias,
        Task parentTask,
        SearchHits hits,
        boolean onlyLocalNode,
        boolean recordFailures,
        boolean mergeLookupFields,
        ActionListener<Void> listener
    ) {

        if (clusterAlias != null && RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias) == false) {
            listener.onResponse(null);
            return;
        }

        if (mergeLookupFields) {
            listener = ActionListener.runBefore(listener, () -> {
                for (SearchHit hit : hits) {
                    hit.getDocumentFields().values().forEach(DocumentField::mergeLookupFieldsToFields);
                }
            });
        }

        final Map<LookupKey, LookupField> resolvedFields = new HashMap<>();
        final Map<LookupKey, List<LookupField>> unresolvedFields = new HashMap<>();
        for (SearchHit hit : hits) {
            for (DocumentField docField : hit.getDocumentFields().values()) {
                for (LookupField lookupField : docField.getLookupFields()) {
                    final LookupKey key = new LookupKey(lookupField);
                    if (lookupField.isResolved()) {
                        resolvedFields.put(key, lookupField);
                    } else {
                        unresolvedFields.computeIfAbsent(key, k -> new ArrayList<>()).add(lookupField);
                    }
                }
            }
        }
        final List<LookupRequest> lookupRequests = new ArrayList<>();
        for (Map.Entry<LookupKey, List<LookupField>> e : unresolvedFields.entrySet()) {
            final LookupField resolved = resolvedFields.get(e.getKey());
            if (resolved != null) {
                if (resolved.getFailure() != null) {
                    e.getValue().forEach(v -> v.setFailure(resolved.getFailure()));
                } else {
                    e.getValue().forEach(v -> v.setValues(resolved.getValues()));
                }
            } else {
                lookupRequests.add(new LookupRequest(e.getKey(), e.getValue()));
            }
        }

        if (lookupRequests.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        if (allowExpensiveQueries.getAsBoolean() == false) {
            handleFailure(
                lookupRequests,
                new ElasticsearchException(
                    "cannot be executed against lookup fields while [" + ALLOW_EXPENSIVE_QUERIES.getKey() + "] is set to [false]."
                )
            );
            listener.onResponse(null);
        }

        final MultiGetRequest multiGetRequest = new MultiGetRequest();
        if (onlyLocalNode) {
            multiGetRequest.preference(Preference.ONLY_LOCAL.type());
        }
        for (LookupRequest lookupRequest : lookupRequests) {
            LookupKey key = lookupRequest.key;
            final MultiGetRequest.Item item = new MultiGetRequest.Item(key.index, key.id).fetchSourceContext(
                new FetchSourceContext(true, key.fields.toArray(new String[0]), Strings.EMPTY_ARRAY)
            );
            multiGetRequest.add(item);
        }

        final ActionListener<Void> finalListener = listener;

        transportService.sendChildRequest(
            transportService.getLocalNodeConnection(),
            MultiGetAction.NAME,
            multiGetRequest,
            parentTask,
            new TransportResponseHandler<MultiGetResponse>() {
                @Override
                public void handleResponse(MultiGetResponse resp) {
                    handleMultiGetResponses(lookupRequests, resp.getResponses(), recordFailures);
                    finalListener.onResponse(null);
                }

                @Override
                public void handleException(TransportException exp) {
                    if (recordFailures) {
                        handleFailure(lookupRequests, exp);
                    }
                    finalListener.onResponse(null);
                }

                @Override
                public MultiGetResponse read(StreamInput in) throws IOException {
                    return new MultiGetResponse(in);
                }
            }
        );
    }

    private static void handleMultiGetResponses(List<LookupRequest> requests, MultiGetItemResponse[] responses, boolean recordFailures) {
        assert requests.size() == responses.length : requests.size() + " != " + responses.length;
        for (int i = 0; i < requests.size(); i++) {
            final LookupRequest request = requests.get(i);
            final List<DocumentField> values;
            final Exception failure;
            if (responses[i].getFailure() != null) {
                if (recordFailures == false) {
                    continue;
                }
                values = null;
                failure = responses[i].getFailure().getFailure();
            } else {
                final GetResponse response = responses[i].getResponse();
                if (response.isExists()) {
                    final Map<String, Object> source = response.getSourceAsMap();
                    values = source.keySet()
                        .stream()
                        .map(field -> new DocumentField(field, XContentMapValues.extractRawValues(field, source)))
                        .toList();
                    failure = null;
                } else {
                    values = null;
                    failure = new ResourceNotFoundException("id [{}] on index [{}] doesn't exist", request.key.id, request.key.index);
                }
            }
            if (failure != null) {
                request.fields.forEach(f -> f.setFailure(failure));
            } else {
                request.fields.forEach(f -> f.setValues(values));
            }
        }
    }

    private static void handleFailure(List<LookupRequest> lookupRequests, Exception ex) {
        for (LookupRequest lookupRequest : lookupRequests) {
            for (LookupField lookupValue : lookupRequest.fields()) {
                lookupValue.setFailure(ex);
            }
        }
    }
}
