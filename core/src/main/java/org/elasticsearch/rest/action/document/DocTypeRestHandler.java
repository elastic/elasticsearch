package org.elasticsearch.rest.action.document;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

/**
 * Wrapper handler for type less APIs to add a default type to the parameters
 */
final class DocTypeRestHandler extends BaseRestHandler {
    private final Delegate delegate;

    DocTypeRestHandler(Settings settings, Delegate delegate) {
        super(settings);
        this.delegate = delegate;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.hasParam("type")) {
            throw new IllegalArgumentException("type parameter is not supported on this handler");
        }
        request.params().put("type", "doc");
        return delegate.prepareRequest(request, client);
    }

    @FunctionalInterface
    public interface Delegate {
        RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException;
    }
}
