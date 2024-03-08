/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class AuthenticationEnrichmentService {
    private final Client client;

    public AuthenticationEnrichmentService(Client client) {
        this.client = client;
    }

    void enrichAuthentication(Authentication authentication, ActionListener<Authentication> listener) {
        // TODO: This needs to be configured somehow (name of source index, principal field and so on)
        // TODO: We can only do this for some realms, add restrictions
        final SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource()
            .version(false)
            .fetchSource(true)
            .trackTotalHits(true)
            .query(QueryBuilders.termQuery("principal", authentication.getEffectiveSubject().getUser().principal()));

        final SearchRequest searchRequest = new SearchRequest(new String[] { ".acl-enrichment-test" }, searchSourceBuilder);
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, TransportSearchAction.TYPE, searchRequest, ActionListener.wrap(searchResponse -> {
            // TODO: Assert on only one hit
            Map<String, Object> source = null;
            if (searchResponse.getHits().getTotalHits().value > 0) {
                source = searchResponse.getHits().getHits()[0].getSourceAsMap();
            }

            // TODO: The idea is that there will be one access control index per connector (authz source) so "access_control_{connector_id}"
            // TODO: Investigate if the naming can cause issues, what happens when overlan with actual metadata keys
            if (source != null && source.containsKey("access_control")) {
                listener.onResponse(authentication.enrichUserMetadata(source.get("access_control")));
            } else {
                listener.onResponse(authentication);
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                listener.onResponse(authentication);
            } else {
                listener.onFailure(exception);
            }
        }));
    }
}
