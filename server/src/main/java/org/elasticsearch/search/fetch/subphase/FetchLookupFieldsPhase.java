/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.MultiSearchAction;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Asynchronously resolves {@link LookupField} that are specified {@link DocumentField#getLookupFields()}
 *
 * @see org.elasticsearch.index.mapper.LookupRuntimeFieldType
 */
public final class FetchLookupFieldsPhase {
    private final TransportService transportService;

    public FetchLookupFieldsPhase(TransportService transportService) {
        this.transportService = transportService;
    }

    /**
     * Asynchronously fetches the lookup fields specified in {@link DocumentField#getLookupFields()} of the given search hits.
     *
     * @param searchTask the parent task of the shard search request
     * @param hits       the search hits
     * @param listener   the listener to be called when the lookup completes
     */
    public void fetchLookupFields(SearchShardTask searchTask, List<SearchHit> hits, ActionListener<Void> listener) {
        final LookupField[] lookupFields = hits.stream()
            .flatMap(hit -> hit.getDocumentFields().values().stream())
            .flatMap(docField -> docField.getLookupFields().stream())
            .distinct()
            .toArray(LookupField[]::new);
        if (lookupFields.length == 0) {
            listener.onResponse(null);
            return;
        }
        final MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        for (LookupField lookupField : lookupFields) {
            multiSearchRequest.add(lookupField.toSearchRequest());
        }
        transportService.sendChildRequest(
            transportService.getLocalNodeConnection(),
            MultiSearchAction.NAME,
            multiSearchRequest,
            searchTask,
            new TransportResponseHandler<MultiSearchResponse>() {
                @Override
                public MultiSearchResponse read(StreamInput in) throws IOException {
                    return new MultiSearchResponse(in);
                }

                @Override
                public void handleResponse(MultiSearchResponse items) {
                    final Map<LookupField, List<Object>> lookupResults = new HashMap<>();
                    Exception failure = null;
                    for (int i = 0; i < items.getResponses().length; i++) {
                        final MultiSearchResponse.Item item = items.getResponses()[i];
                        final LookupField lookupField = lookupFields[i];
                        if (item.isFailure()) {
                            failure = ExceptionsHelper.useOrSuppress(failure, item.getFailure());
                        } else if (failure == null) {
                            final List<Object> fetchedValues = new ArrayList<>();
                            for (SearchHit rightHit : item.getResponse().getHits()) {
                                final Map<String, List<Object>> fetchedFields = rightHit.getDocumentFields()
                                    .values()
                                    .stream()
                                    .collect(Collectors.toMap(DocumentField::getName, DocumentField::getValues));
                                if (fetchedFields.isEmpty() == false) {
                                    fetchedValues.add(fetchedFields);
                                }
                            }
                            lookupResults.put(lookupField, fetchedValues);
                        }
                    }
                    if (failure != null) {
                        listener.onFailure(failure);
                    } else {
                        for (SearchHit hit : hits) {
                            hit.resolveLookupFields(lookupResults);
                        }
                        listener.onResponse(null);
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }
            }
        );
    }
}
