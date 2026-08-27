/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.inference.action.GetRegionPolicyAction;
import org.elasticsearch.xpack.core.inference.action.RegionPolicyResponse;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyDoc;
import org.elasticsearch.xpack.inference.InferenceIndex;

import java.io.IOException;

public class TransportGetRegionPolicyAction extends HandledTransportAction<GetRegionPolicyAction.Request, RegionPolicyResponse> {

    private final OriginSettingClient client;

    @Inject
    public TransportGetRegionPolicyAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            GetRegionPolicyAction.NAME,
            transportService,
            actionFilters,
            GetRegionPolicyAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = new OriginSettingClient(client, ClientHelper.INFERENCE_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, GetRegionPolicyAction.Request request, ActionListener<RegionPolicyResponse> finalListener) {
        searchAndProcessRegionPolicy(finalListener);
    }

    private void searchAndProcessRegionPolicy(ActionListener<RegionPolicyResponse> listener) {
        doSearchRegionPolicy(
            client,
            false,
            listener.<SearchResponse>delegateFailureAndWrap((l, searchResponse) -> processSearchResponse(searchResponse.getHits(), l))
                .delegateResponse((l, e) -> {
                    if (e instanceof IndexNotFoundException) {
                        l.onFailure(noRegionPolicyConfiguredException());
                    } else {
                        l.onFailure(e);
                    }
                })
        );
    }

    public static void doSearchRegionPolicy(Client client, boolean requestSeqNoAndPrimaryTerm, ActionListener<SearchResponse> listener) {
        client.prepareSearch(InferenceIndex.INDEX_ALIAS)
            .setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.idsQuery().addIds(RegionPolicyDoc.DOCUMENT_ID)))
            .setAllowPartialSearchResults(false)
            .seqNoAndPrimaryTerm(requestSeqNoAndPrimaryTerm)
            .execute(listener);
    }

    public static ResourceNotFoundException noRegionPolicyConfiguredException() {
        return new ResourceNotFoundException("No region policy is configured for this deployment");
    }

    private void processSearchResponse(SearchHits searchHits, ActionListener<RegionPolicyResponse> listener) {
        SearchHit[] hits = searchHits.getHits();
        assert hits.length <= 1 : "multiple region policies found when only one is expected";
        if (hits.length == 0) {
            listener.onFailure(noRegionPolicyConfiguredException());
        } else {
            listener.onResponse(new RegionPolicyResponse(parseRegionPolicy(hits[0])));
        }
    }

    public static RegionPolicyDoc parseRegionPolicy(SearchHit hit) {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                hit.getSourceRef(),
                XContentType.JSON
            )
        ) {
            return RegionPolicyDoc.LENIENT_PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse region policy with id [{}]", e, RegionPolicyDoc.DOCUMENT_ID);
        }
    }
}
