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

package co.elastic.elasticsearch.stateless.cache;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.action.ClearBlobCacheNodesRequest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Rest endpoint for clearing all blob caches. This is intended for use with benchmark testing in cold-cache scenarios.
 */
@ServerlessScope(Scope.INTERNAL)
public class ClearBlobCacheRestHandler extends BaseRestHandler {
    @Override
    public String getName() {
        return "clear_blob_cache";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_internal/blob_caches/clear"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        return restChannel -> client.execute(
            Stateless.CLEAR_BLOB_CACHE_ACTION,
            new ClearBlobCacheNodesRequest(),
            new RestChunkedToXContentListener<>(restChannel)
        );
    }
}
