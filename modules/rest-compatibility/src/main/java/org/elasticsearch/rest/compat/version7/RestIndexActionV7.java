package org.elasticsearch.rest.compat.version7;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.document.RestIndexAction;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class RestIndexActionV7 {
    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in document " +
        "index requests is deprecated, use the typeless endpoints instead (/{index}/_doc/{id}, /{index}/_doc, " +
        "or /{index}/_create/{id}).";
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
        LogManager.getLogger(RestIndexAction.class));

    private static void logDeprecationMessage() {
        deprecationLogger.deprecatedAndMaybeLog("index_with_types", TYPES_DEPRECATION_MESSAGE);
    }

    public static class CompatibleRestIndexAction extends RestIndexAction {
        @Override
        public List<Route> routes() {
            assert Version.CURRENT.major == 8 : "REST API compatilbity for version 7 is only supported on version 8";

            return List.of(
                new Route(POST, "/{index}/{type}/{id}"),
                new Route(PUT, "/{index}/{type}/{id}"));
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            logDeprecationMessage();
            request.param("type");
            return super.prepareRequest(request, client);
        }


        @Override
        public boolean compatibilityRequired() {
            return true;
        }
    }

    public static class CompatibleCreateHandler extends RestIndexAction.CreateHandler {
        @Override
        public List<Route> routes() {
            return unmodifiableList(asList(
                new Route(POST, "/{index}/{type}/{id}/_create"),
                new Route(PUT, "/{index}/{type}/{id}/_create")));
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            logDeprecationMessage();
            request.param("type");
            return super.prepareRequest(request, client);
        }

        @Override
        public boolean compatibilityRequired() {
            return true;
        }
    }

    public static final class CompatibleAutoIdHandler extends RestIndexAction.AutoIdHandler {

        public CompatibleAutoIdHandler(Supplier<DiscoveryNodes> nodesInCluster) {
            super(nodesInCluster);
        }

        @Override
        public List<Route> routes() {
            return singletonList(new Route(POST, "/{index}/{type}"));
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
            logDeprecationMessage();
            request.param("type");
            return super.prepareRequest(request, client);
        }

        @Override
        public boolean compatibilityRequired() {
            return true;
        }
    }
}
