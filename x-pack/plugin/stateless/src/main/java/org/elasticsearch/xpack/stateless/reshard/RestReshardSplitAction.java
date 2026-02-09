/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.EmptyResponseListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.INTERNAL)
public class RestReshardSplitAction extends BaseRestHandler {

    public static final Setting<Boolean> RESHARD_ALLOWED = boolSetting("rest.internal.reshard_allowed", false, Setting.Property.NodeScope);
    private final boolean reshardAllowed;

    public RestReshardSplitAction(Settings settings) {
        reshardAllowed = RESHARD_ALLOWED.get(settings);
    }

    @Override
    public String getName() {
        return "reshard_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_internal/reshard/split"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (reshardAllowed == false) {
            throw new IllegalStateException("reshard is not allowed");
        }
        ReshardIndexRequest actionRequest = ReshardIndexRequest.fromXContent(request.contentParser());
        return channel -> client.execute(TransportReshardAction.TYPE, actionRequest, new EmptyResponseListener(channel));
    }
}
