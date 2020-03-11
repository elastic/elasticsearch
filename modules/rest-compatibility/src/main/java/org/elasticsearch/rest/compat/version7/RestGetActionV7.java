package org.elasticsearch.rest.compat.version7;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.CompatibleHandlers;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.document.RestGetAction;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

public class RestGetActionV7 extends RestGetAction {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestGetAction.class));
    private static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in "
        + "document get requests is deprecated, use the /{index}/_doc/{id} endpoint instead.";
    private static final Consumer<RestRequest> DEPRECATION_WARNING = r -> deprecationLogger.deprecatedAndMaybeLog(
        "get_with_types",
        TYPES_DEPRECATION_MESSAGE
    );

    @Override
    public List<Route> routes() {
        assert Version.CURRENT.major == 8 : "REST API compatibility for version 7 is only supported on version 8";

        return unmodifiableList(asList(new Route(GET, "/{index}/{type}/{id}"), new Route(HEAD, "/{index}/{type}/{id}")));
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, final NodeClient client) throws IOException {
        DEPRECATION_WARNING.accept(request);
        CompatibleHandlers.consumeParameterType(deprecationLogger).accept(request);
        return super.prepareRequest(request, client);
    }



    @Override
    public boolean compatibilityRequired() {
        return true;
    }
}
