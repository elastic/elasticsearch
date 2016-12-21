package org.elasticsearch.transport;

import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;

// nocommit javadocs
public final class TransportActionProxy {

    private TransportActionProxy() {} // no instance

    private static class ProxyRequestHandler<T extends ProxyRequest> implements TransportRequestHandler<T> {

        private final TransportService service;
        private final String action;
        private final Supplier<TransportResponse> responseFactory;

        public ProxyRequestHandler(TransportService service, String action, Supplier<TransportResponse> responseFactory) {
            this.service = service;
            this.action = action;
            this.responseFactory = responseFactory;
        }

        @Override
        public void messageReceived(T request, TransportChannel channel) throws Exception {
            DiscoveryNode targetNode = request.targetNode;
            TransportRequest wrappedRequest = request.wrapped;
            service.sendRequest(targetNode, action, wrappedRequest, new ProxyResponseHandler<>(channel, responseFactory));
        }
    }

    private static class ProxyResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        private final Supplier<T> responseFactory;
        private final TransportChannel channel;

        public ProxyResponseHandler(TransportChannel channel, Supplier<T> responseFactory) {
            this.responseFactory = responseFactory;
            this.channel = channel;

        }
        @Override
        public T newInstance() {
            return responseFactory.get();
        }

        @Override
        public void handleResponse(T response) {
            try {
                channel.sendResponse(response);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void handleException(TransportException exp) {
            try {
                channel.sendResponse(exp);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }

    static class ProxyRequest<T extends TransportRequest> extends TransportRequest {
        T wrapped;
        Supplier<T> supplier;
        DiscoveryNode targetNode;

        public ProxyRequest(Supplier<T>  supplier) {
            this.supplier = supplier;
        }

        public ProxyRequest(T wrapped, DiscoveryNode targetNode) {
            this.wrapped = wrapped;
            this.targetNode = targetNode;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            targetNode = new DiscoveryNode(in);
            wrapped = supplier.get();
            wrapped.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            targetNode.writeTo(out);
            wrapped.writeTo(out);
        }
    }

    /**
     * Registers a proxy request handler that allows to forward requests for the given action to another node.
     */
    public static String registerProxyAction(TransportService service, String action, Supplier<TransportResponse> responseSupplier) {
        RequestHandlerRegistry requestHandler = service.getRequestHandler(action);
        String proxyAction = "internal:transport/proxy/" + action;
        service.registerRequestHandler(proxyAction, () -> new ProxyRequest(requestHandler::newRequest), ThreadPool.Names.SAME, true, false
            , new ProxyRequestHandler<>(service, action, responseSupplier));
        return proxyAction;
    }

    //nocommit javadocs
    public static void sendProxyRequest(TransportService service, DiscoveryNode proxyNode, DiscoveryNode targetNode, String action,
                                 TransportRequest request, TransportResponseHandler handler) {
        String proxyAction = "internal:transport/proxy/" + action;
        service.sendRequest(proxyNode, proxyAction, new ProxyRequest(request, targetNode), handler);
    }
}
