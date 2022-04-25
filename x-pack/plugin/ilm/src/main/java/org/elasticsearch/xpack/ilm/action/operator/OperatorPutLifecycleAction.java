/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action.operator;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.operator.OperatorHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;
import org.elasticsearch.xpack.ilm.action.TransportPutLifecycleAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * TODO: Add docs
 */
public class OperatorPutLifecycleAction implements OperatorHandler<PutLifecycleAction.Request> {

    private final NamedXContentRegistry xContentRegistry;
    private final Client client;
    private final XPackLicenseState licenseState;

    public static final String KEY = "ilm";

    public OperatorPutLifecycleAction(NamedXContentRegistry xContentRegistry, Client client, XPackLicenseState licenseState) {
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.licenseState = licenseState;
    }

    @Override
    public String key() {
        return KEY;
    }

    @SuppressWarnings("unchecked")
    public Collection<PutLifecycleAction.Request> prepare(Object input) throws IOException {
        List<PutLifecycleAction.Request> result = new ArrayList<>();

        Map<String, ?> source = asMap(input);

        for (String name : source.keySet()) {
            Map<String, ?> content = (Map<String, ?>) source.get(name);
            try (XContentParser parser = mapToXContentParser(content)) {
                PutLifecycleAction.Request request = PutLifecycleAction.Request.parseRequest(name, parser);
                validate(request);
                result.add(request);
            }
        }

        return result;
    }

    @Override
    public ClusterState transform(Object source, ClusterState state) throws Exception {
        var requests = prepare(source);

        for (var request : requests) {
            TransportPutLifecycleAction.UpdateLifecycleTask task = new TransportPutLifecycleAction.UpdateLifecycleTask(
                request,
                licenseState,
                xContentRegistry,
                client
            );

            state = task.execute(state);
        }
        return state;
    }
}
