/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleRequest;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestPutLifecycleAction extends BaseRestHandler {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(RestPutLifecycleAction.class);

    public static final String MAX_SIZE_DEPRECATION_MESSAGE = "Use of the [max_size] rollover condition found in phase [{}]. This"
        + " condition has been deprecated in favour of the [max_primary_shard_size] condition and will be removed in a later version";

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_ilm/policy/{name}"));
    }

    @Override
    public String getName() {
        return "ilm_put_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        final PutLifecycleRequest putLifecycleRequest;
        try (var parser = restRequest.contentParser()) {
            putLifecycleRequest = PutLifecycleRequest.parseRequest(new PutLifecycleRequest.Factory() {
                @Override
                public PutLifecycleRequest create(LifecyclePolicy lifecyclePolicy) {
                    return new PutLifecycleRequest(getMasterNodeTimeout(restRequest), getAckTimeout(restRequest), lifecyclePolicy);
                }

                @Override
                public String getPolicyName() {
                    return restRequest.param("name");
                }
            }, parser);
        }

        // Check for deprecated rollover conditions
        for (Phase phase : putLifecycleRequest.getPolicy().getPhases().values()) {
            for (LifecycleAction actionObj : phase.getActions().values()) {
                if (actionObj instanceof RolloverAction rolloverAction) {
                    if (rolloverAction.getConditions().getMaxSize() != null) {
                        DEPRECATION_LOGGER.warn(
                            DeprecationCategory.API,
                            "rollover-max-size-condition",
                            MAX_SIZE_DEPRECATION_MESSAGE,
                            phase.getName()
                        );
                    }
                }
            }
        }

        return channel -> client.execute(ILMActions.PUT, putLifecycleRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of("max_size_deprecation", "searchable_snapshot_force_merge_on_clone");
    }
}
