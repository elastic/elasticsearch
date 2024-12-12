/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.INTERNAL)
public class RestCloseIndexAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestCloseIndexAction.class);

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_close"), new Route(POST, "/{index}/_close"));
    }

    @Override
    public String getName() {
        return "close_index_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(Strings.splitStringByCommaToArray(request.param("index")));
        closeIndexRequest.masterNodeTimeout(getMasterNodeTimeout(request));
        closeIndexRequest.ackTimeout(getAckTimeout(request));
        closeIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, closeIndexRequest.indicesOptions()));
        handleIndexSettingWaitForActiveShards(request, closeIndexRequest);
        return channel -> client.admin().indices().close(closeIndexRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Special handling of the query param 'wait_for_active_shards' because of the deprecated param value 'index-setting'.
     * - In 10.x this code should be simplified to simply parsing the active shard count with {@link ActiveShardCount#parseString(String)}
     * - In 9.x we throw an error that informs the user about the parameter value removal; unless we are running in rest-compatibility mode
     * with 8.x when we log a deprecation warning and handle the value as a noop.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    private void handleIndexSettingWaitForActiveShards(final RestRequest request, final CloseIndexRequest closeIndexRequest) {
        String waitForActiveShards = request.param("wait_for_active_shards");
        if (waitForActiveShards == null) {
            return;
        }
        if ("index-setting".equalsIgnoreCase(waitForActiveShards)) {
            String deprecationMessage =
                "?wait_for_active_shards=index-setting is now the default behaviour; the 'index-setting' value for this parameter "
                    + "should no longer be used since it is not supported in version "
                    + Version.V_9_0_0.major;
            if (request.getRestApiVersion() == RestApiVersion.V_8) {
                deprecationLogger.warn(
                    DeprecationCategory.SETTINGS,
                    "close-index-wait_for_active_shards-index-setting",
                    deprecationMessage
                );
            } else {
                throw new IllegalArgumentException(deprecationMessage);
            }
        } else {
            closeIndexRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
        }
    }
}
