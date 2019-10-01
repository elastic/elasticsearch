/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.EnrichStore;

import java.util.List;
import java.util.stream.Collectors;

public class TransportGetEnrichPolicyAction extends HandledTransportAction<GetEnrichPolicyAction.Request,
    GetEnrichPolicyAction.Response> {

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportGetEnrichPolicyAction(TransportService transportService,
                                          ClusterService clusterService,
                                          ActionFilters actionFilters,
                                          Client client) {
        super(GetEnrichPolicyAction.NAME, transportService, actionFilters, GetEnrichPolicyAction.Request::new);
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, GetEnrichPolicyAction.Request request, ActionListener<GetEnrichPolicyAction.Response> listener) {
        EnrichStore.getPolicies(clusterService.state(), client, ActionListener.wrap(
            resp -> {
                if (request.getNames() == null || request.getNames().isEmpty()) {
                    listener.onResponse(new GetEnrichPolicyAction.Response(resp));
                } else {
                    List<EnrichPolicy.NamedPolicy> policies = resp.stream().filter(
                        policy -> {
                            return request.getNames().contains(policy.getName());
                        }
                    ).collect(Collectors.toList());
                    listener.onResponse(new GetEnrichPolicyAction.Response(policies));
                }
            },
            listener::onFailure
        ));
    }
}
