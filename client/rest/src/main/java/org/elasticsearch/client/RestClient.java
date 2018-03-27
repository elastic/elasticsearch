/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.AuthCache;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;

import static java.util.Collections.singletonList;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client that connects to an Elasticsearch cluster through HTTP.
 * <p>
 * Must be created using {@link RestClientBuilder}, which allows to set all the different options or just rely on defaults.
 * The hosts that are part of the cluster need to be provided at creation time, but can also be replaced later
 * by calling {@link #setHosts(HttpHost...)}.
 * <p>
 * The method {@link #performRequest(String, String, Map, HttpEntity, Header...)} allows to send a request to the cluster. When
 * sending a request, a host gets selected out of the provided ones in a round-robin fashion. Failing hosts are marked dead and
 * retried after a certain amount of time (minimum 1 minute, maximum 30 minutes), depending on how many times they previously
 * failed (the more failures, the later they will be retried). In case of failures all of the alive nodes (or dead nodes that
 * deserve a retry) are retried until one responds or none of them does, in which case an {@link IOException} will be thrown.
 * <p>
 * Requests can be either synchronous or asynchronous. The asynchronous variants all end with {@code Async}.
 * <p>
 * Requests can be traced by enabling trace logging for "tracer". The trace logger outputs requests and responses in curl format.
 */
public class RestClient extends AbstractRestClientActions implements Closeable {
    private static final Log logger = LogFactory.getLog(RestClient.class);

    /**
     * The maximum number of attempts that {@link #nextNode(NodeSelector)} makes
     * before giving up and failing the request.
     */
    private static final int MAX_NEXT_NODES_ATTEMPTS = 10;

    private final CloseableHttpAsyncClient client;
    // We don't rely on default headers supported by HttpAsyncClient as those cannot be replaced.
    // These are package private for tests.
    final List<Header> defaultHeaders;
    private final long maxRetryTimeoutMillis;
    private final String pathPrefix;
    private final AtomicInteger lastNodeIndex = new AtomicInteger(0);
    private final ConcurrentMap<HttpHost, DeadHostState> blacklist = new ConcurrentHashMap<>();
    private final FailureListener failureListener;
    private volatile NodeTuple<List<Node>> nodeTuple;

    RestClient(CloseableHttpAsyncClient client, long maxRetryTimeoutMillis, Header[] defaultHeaders,
               Node[] nodes, String pathPrefix, FailureListener failureListener) {
        this.client = client;
        this.maxRetryTimeoutMillis = maxRetryTimeoutMillis;
        this.defaultHeaders = Collections.unmodifiableList(Arrays.asList(defaultHeaders));
        this.failureListener = failureListener;
        this.pathPrefix = pathPrefix;
        setNodes(nodes);
    }

    /**
     * Returns a new {@link RestClientBuilder} to help with {@link RestClient} creation.
     * Creates a new builder instance and sets the nodes that the client will send requests to.
     * @see Node#Node(HttpHost)
     */
    public static RestClientBuilder builder(HttpHost... hosts) {
        return builder(hostsToNodes(hosts));
    }

    /**
     * Returns a new {@link RestClientBuilder} to help with {@link RestClient} creation.
     * Creates a new builder instance and sets the nodes that the client will send requests to.
     */
    public static RestClientBuilder builder(Node... nodes) {
        return new RestClientBuilder(nodes);
    }

    /**
     * Replaces the nodes that the client communicates without providing any
     * metadata about any of the nodes.
     * @see Node#Node(HttpHost)
     */
    public void setHosts(HttpHost... hosts) {
        setNodes(hostsToNodes(hosts));
    }

    private static Node[] hostsToNodes(HttpHost[] hosts) {
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException("hosts must not be null or empty");
        }
        Node[] nodes = new Node[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            nodes[i] = new Node(hosts[i]);
        }
        return nodes;
    }

    /**
     * Replaces the nodes that the client communicates with. Prefer this to
     * {@link #setHosts(HttpHost...)} if you have metadata about the hosts
     * like their Elasticsearch version of which roles they implement.
     */
    public synchronized void setNodes(Node... nodes) {
        if (nodes == null || nodes.length == 0) {
            throw new IllegalArgumentException("nodes must not be null or empty");
        }
        AuthCache authCache = new BasicAuthCache();

        for (Node node : nodes) {
            if (node == null) {
                throw new IllegalArgumentException("node cannot be null");
            }
            authCache.put(node.getHost(), new BasicScheme());
        }
        this.nodeTuple = new NodeTuple<>(Collections.unmodifiableList(
            Arrays.asList(nodes)), authCache);
        this.blacklist.clear();
    }

    /**
     * Copy of the list of nodes that the client knows about.
     */
    public Node[] getNodes() { // TODO is it ok to expose this? It feels excessive but we do use it in tests.
        return nodeTuple.nodes.toArray(new Node[0]);
    }

    @Override
    final SyncResponseListener syncResponseListener() {
        return new SyncResponseListener(maxRetryTimeoutMillis);
    }

    @Override
    public RestClientActions withNodeSelector(NodeSelector nodeSelector) {
        return new RestClientView(this, nodeSelector);
    }

    // TODO this exists entirely to so we don't have to change much in the high level rest client tests. We'll remove in a followup.
    @Override
    public Response performRequest(String method, String endpoint, Map<String, String> params,
                                         HttpEntity entity, Header... headers) throws IOException {
        return super.performRequest(method, endpoint, params, entity, headers);
    }

    // TODO this exists entirely to so we don't have to change much in the high level rest client tests. We'll remove in a followup.
    @Override
    public void performRequestAsync(String method, String endpoint, Map<String, String> params,
                                          HttpEntity entity, ResponseListener responseListener, Header... headers) {
        super.performRequestAsync(method, endpoint, params, entity, responseListener, headers);
    }

    @Override
    final void performRequestAsyncNoCatch(String method, String endpoint, Map<String, String> params,
            HttpEntity entity, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
            ResponseListener responseListener, Header[] headers) throws IOException {
        // Requests made directly to the client use the noop NodeSelector.
        performRequestAsyncNoCatch(method, endpoint, params, entity, httpAsyncResponseConsumerFactory,
            responseListener, NodeSelector.ANY, headers);
    }

    void performRequestAsyncNoCatch(String method, String endpoint, Map<String, String> params,
                HttpEntity entity, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
                ResponseListener responseListener, NodeSelector nodeSelector, Header[] headers) throws IOException {
        Objects.requireNonNull(params, "params must not be null");
        Map<String, String> requestParams = new HashMap<>(params);
        //ignore is a special parameter supported by the clients, shouldn't be sent to es
        String ignoreString = requestParams.remove("ignore");
        Set<Integer> ignoreErrorCodes;
        if (ignoreString == null) {
            if (HttpHead.METHOD_NAME.equals(method)) {
                //404 never causes error if returned for a HEAD request
                ignoreErrorCodes = Collections.singleton(404);
            } else {
                ignoreErrorCodes = Collections.emptySet();
            }
        } else {
            String[] ignoresArray = ignoreString.split(",");
            ignoreErrorCodes = new HashSet<>();
            if (HttpHead.METHOD_NAME.equals(method)) {
                //404 never causes error if returned for a HEAD request
                ignoreErrorCodes.add(404);
            }
            for (String ignoreCode : ignoresArray) {
                try {
                    ignoreErrorCodes.add(Integer.valueOf(ignoreCode));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("ignore value should be a number, found [" + ignoreString + "] instead", e);
                }
            }
        }
        URI uri = buildUri(pathPrefix, endpoint, requestParams);
        HttpRequestBase request = createHttpRequest(method, uri, entity);
        setHeaders(request, headers);
        FailureTrackingResponseListener failureTrackingResponseListener = new FailureTrackingResponseListener(responseListener);
        long startTime = System.nanoTime();
        performRequestAsync(startTime, nextNode(nodeSelector), request, ignoreErrorCodes, httpAsyncResponseConsumerFactory,
                failureTrackingResponseListener);
    }

    private void performRequestAsync(final long startTime, final NodeTuple<Iterator<HttpHost>> hostTuple, final HttpRequestBase request,
                                     final Set<Integer> ignoreErrorCodes,
                                     final HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
                                     final FailureTrackingResponseListener listener) {
        final HttpHost host = hostTuple.nodes.next();
        //we stream the request body if the entity allows for it
        final HttpAsyncRequestProducer requestProducer = HttpAsyncMethods.create(host, request);
        final HttpAsyncResponseConsumer<HttpResponse> asyncResponseConsumer =
            httpAsyncResponseConsumerFactory.createHttpAsyncResponseConsumer();
        final HttpClientContext context = HttpClientContext.create();
        context.setAuthCache(hostTuple.authCache);
        client.execute(requestProducer, asyncResponseConsumer, context, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(HttpResponse httpResponse) {
                try {
                    RequestLogger.logResponse(logger, request, host, httpResponse);
                    int statusCode = httpResponse.getStatusLine().getStatusCode();
                    Response response = new Response(request.getRequestLine(), host, httpResponse);
                    if (isSuccessfulResponse(statusCode) || ignoreErrorCodes.contains(response.getStatusLine().getStatusCode())) {
                        onResponse(host);
                        listener.onSuccess(response);
                    } else {
                        ResponseException responseException = new ResponseException(response);
                        if (isRetryStatus(statusCode)) {
                            //mark host dead and retry against next one
                            onFailure(host);
                            retryIfPossible(responseException);
                        } else {
                            //mark host alive and don't retry, as the error should be a request problem
                            onResponse(host);
                            listener.onDefinitiveFailure(responseException);
                        }
                    }
                } catch(Exception e) {
                    listener.onDefinitiveFailure(e);
                }
            }

            @Override
            public void failed(Exception failure) {
                try {
                    RequestLogger.logFailedRequest(logger, request, host, failure);
                    onFailure(host);
                    retryIfPossible(failure);
                } catch(Exception e) {
                    listener.onDefinitiveFailure(e);
                }
            }

            private void retryIfPossible(Exception exception) {
                if (hostTuple.nodes.hasNext()) {
                    //in case we are retrying, check whether maxRetryTimeout has been reached
                    long timeElapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                    long timeout = maxRetryTimeoutMillis - timeElapsedMillis;
                    if (timeout <= 0) {
                        IOException retryTimeoutException = new IOException(
                                "request retries exceeded max retry timeout [" + maxRetryTimeoutMillis + "]");
                        listener.onDefinitiveFailure(retryTimeoutException);
                    } else {
                        listener.trackFailure(exception);
                        request.reset();
                        performRequestAsync(startTime, hostTuple, request, ignoreErrorCodes, httpAsyncResponseConsumerFactory, listener);
                    }
                } else {
                    listener.onDefinitiveFailure(exception);
                }
            }

            @Override
            public void cancelled() {
                listener.onDefinitiveFailure(new ExecutionException("request was cancelled", null));
            }
        });
    }

    private void setHeaders(HttpRequest httpRequest, Header[] requestHeaders) {
        Objects.requireNonNull(requestHeaders, "request headers must not be null");
        // request headers override default headers, so we don't add default headers if they exist as request headers
        final Set<String> requestNames = new HashSet<>(requestHeaders.length);
        for (Header requestHeader : requestHeaders) {
            Objects.requireNonNull(requestHeader, "request header must not be null");
            httpRequest.addHeader(requestHeader);
            requestNames.add(requestHeader.getName());
        }
        for (Header defaultHeader : defaultHeaders) {
            if (requestNames.contains(defaultHeader.getName()) == false) {
                httpRequest.addHeader(defaultHeader);
            }
        }
    }

    /**
     * Returns a non-empty {@link Iterator} of nodes to be used for a request
     * that match the {@link NodeSelector}.
     * <p>
     * If there are no living nodes that match the {@link NodeSelector}
     * this will return the dead node that matches the {@link NodeSelector}
     * that is closest to being revived.
     * <p>
     * If no living and no dead nodes match the selector we retry a few
     * times to handle concurrent modifications of the list of dead nodes.
     * We never block the thread or {@link Thread#sleep} or anything like
     * that. If the retries fail this throws a {@link IOException}.
     * @throws IOException if no nodes are available
     */
    private NodeTuple<Iterator<HttpHost>> nextNode(NodeSelector nodeSelector) throws IOException {
        NodeTuple<List<Node>> nodeTuple = this.nodeTuple;
        List<HttpHost> hosts = selectHosts(nodeTuple, blacklist, lastNodeIndex, System.nanoTime(), nodeSelector);
        return new NodeTuple<>(hosts.iterator(), nodeTuple.authCache);
    }

    static List<HttpHost> selectHosts(NodeTuple<List<Node>> nodeTuple,
            Map<HttpHost, DeadHostState> blacklist, AtomicInteger lastNodeIndex,
            long now, NodeSelector nodeSelector) throws IOException {
        class DeadNodeAndRevival {
            final Node node;
            final long nanosUntilRevival;

            DeadNodeAndRevival(Node node, long nanosUntilRevival) {
                this.node = node;
                this.nanosUntilRevival = nanosUntilRevival;
            }

            @Override
            public String toString() {
                return node.toString();
            }
        }

        /*
         * Sort the nodes into living and dead lists.
         */
        List<Node> livingNodes = new ArrayList<>(nodeTuple.nodes.size() - blacklist.size());
        List<DeadNodeAndRevival> deadNodes = new ArrayList<>(blacklist.size());
        for (Node node : nodeTuple.nodes) {
            DeadHostState deadness = blacklist.get(node.getHost());
            if (deadness == null) {
                livingNodes.add(node);
                continue;
            }
            long nanosUntilRevival = now - deadness.getDeadUntilNanos();
            if (nanosUntilRevival > 0) {
                livingNodes.add(node);
                continue;
            }
            deadNodes.add(new DeadNodeAndRevival(node, nanosUntilRevival));
        }

        if (false == livingNodes.isEmpty()) {
            /*
             * Normal state: there is at least one living node. Rotate the
             * list so subsequent requests to will see prefer the nodes in
             * a different order then run them through the NodeSelector so
             * it can have its say in which nodes are ok and their ordering.
             * If the selector is ok with any over the living nodes then use
             * them for the request.
             */
            Collections.rotate(livingNodes, lastNodeIndex.getAndIncrement());
            List<Node> selectedLivingNodes = nodeSelector.select(livingNodes);
            if (false == selectedLivingNodes.isEmpty()) {
                List<HttpHost> hosts = new ArrayList<>(selectedLivingNodes.size());
                for (Node node : selectedLivingNodes) {
                    hosts.add(node.getHost());
                }
                return hosts;
            }
        }

        /*
         * Last resort: If there are no good nodes to use, either because
         * the selector rejected all the living nodes or because there aren't
         * any living ones. Either way, we want to revive a single dead node
         * that the NodeSelectors are OK with. We do this by sorting the dead
         * nodes by their revival time and passing them through the
         * NodeSelector so it can have its say in which nodes are ok and their
         * ordering. If the selector is ok with any of the nodes then use just
         * the first one in the list because we only want to revive a single
         * node.
         */
        if (false == deadNodes.isEmpty()) {
            Collections.sort(deadNodes, new Comparator<DeadNodeAndRevival>() {
                @Override
                public int compare(DeadNodeAndRevival lhs, DeadNodeAndRevival rhs) {
                    return Long.compare(rhs.nanosUntilRevival, lhs.nanosUntilRevival);
                }
            });

            List<Node> selectedDeadNodes = new ArrayList<>(deadNodes.size());
            for (DeadNodeAndRevival n : deadNodes) {
                selectedDeadNodes.add(n.node);
            }
            selectedDeadNodes = nodeSelector.select(selectedDeadNodes);
            if (false == selectedDeadNodes.isEmpty()) {
                return singletonList(selectedDeadNodes.get(0).getHost());
            }
        }
        throw new IOException("NodeSelector [" + nodeSelector + "] rejected all nodes, "
                + "living " + livingNodes + " and dead " + deadNodes);
    }

    /**
     * Called after each successful request call.
     * Receives as an argument the host that was used for the successful request.
     */
    private void onResponse(HttpHost host) {
        DeadHostState removedHost = this.blacklist.remove(host);
        if (logger.isDebugEnabled() && removedHost != null) {
            logger.debug("removed host [" + host + "] from blacklist");
        }
    }

    /**
     * Called after each failed attempt.
     * Receives as an argument the host that was used for the failed attempt.
     */
    private void onFailure(HttpHost host) throws IOException {
        while(true) {
            DeadHostState previousDeadHostState = blacklist.putIfAbsent(host, DeadHostState.INITIAL_DEAD_STATE);
            if (previousDeadHostState == null) {
                logger.debug("added host [" + host + "] to blacklist");
                break;
            }
            if (blacklist.replace(host, previousDeadHostState, new DeadHostState(previousDeadHostState))) {
                logger.debug("updated host [" + host + "] already in blacklist");
                break;
            }
        }
        failureListener.onFailure(host);
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private static boolean isSuccessfulResponse(int statusCode) {
        return statusCode < 300;
    }

    private static boolean isRetryStatus(int statusCode) {
        switch(statusCode) {
            case 502:
            case 503:
            case 504:
                return true;
        }
        return false;
    }

    private static Exception addSuppressedException(Exception suppressedException, Exception currentException) {
        if (suppressedException != null) {
            currentException.addSuppressed(suppressedException);
        }
        return currentException;
    }

    private static HttpRequestBase createHttpRequest(String method, URI uri, HttpEntity entity) {
        switch(method.toUpperCase(Locale.ROOT)) {
            case HttpDeleteWithEntity.METHOD_NAME:
                return addRequestBody(new HttpDeleteWithEntity(uri), entity);
            case HttpGetWithEntity.METHOD_NAME:
                return addRequestBody(new HttpGetWithEntity(uri), entity);
            case HttpHead.METHOD_NAME:
                return addRequestBody(new HttpHead(uri), entity);
            case HttpOptions.METHOD_NAME:
                return addRequestBody(new HttpOptions(uri), entity);
            case HttpPatch.METHOD_NAME:
                return addRequestBody(new HttpPatch(uri), entity);
            case HttpPost.METHOD_NAME:
                HttpPost httpPost = new HttpPost(uri);
                addRequestBody(httpPost, entity);
                return httpPost;
            case HttpPut.METHOD_NAME:
                return addRequestBody(new HttpPut(uri), entity);
            case HttpTrace.METHOD_NAME:
                return addRequestBody(new HttpTrace(uri), entity);
            default:
                throw new UnsupportedOperationException("http method not supported: " + method);
        }
    }

    private static HttpRequestBase addRequestBody(HttpRequestBase httpRequest, HttpEntity entity) {
        if (entity != null) {
            if (httpRequest instanceof HttpEntityEnclosingRequestBase) {
                ((HttpEntityEnclosingRequestBase)httpRequest).setEntity(entity);
            } else {
                throw new UnsupportedOperationException(httpRequest.getMethod() + " with body is not supported");
            }
        }
        return httpRequest;
    }

    static URI buildUri(String pathPrefix, String path, Map<String, String> params) {
        Objects.requireNonNull(path, "path must not be null");
        try {
            String fullPath;
            if (pathPrefix != null) {
                if (path.startsWith("/")) {
                    fullPath = pathPrefix + path;
                } else {
                    fullPath = pathPrefix + "/" + path;
                }
            } else {
                fullPath = path;
            }

            URIBuilder uriBuilder = new URIBuilder(fullPath);
            for (Map.Entry<String, String> param : params.entrySet()) {
                uriBuilder.addParameter(param.getKey(), param.getValue());
            }
            return uriBuilder.build();
        } catch(URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * Listener used in any async call to wrap the provided user listener (or SyncResponseListener in sync calls).
     * Allows to track potential failures coming from the different retry attempts and returning to the original listener
     * only when we got a response (successful or not to be retried) or there are no hosts to retry against.
     */
    static class FailureTrackingResponseListener {
        private final ResponseListener responseListener;
        private volatile Exception exception;

        FailureTrackingResponseListener(ResponseListener responseListener) {
            this.responseListener = responseListener;
        }

        /**
         * Notifies the caller of a response through the wrapped listener
         */
        void onSuccess(Response response) {
            responseListener.onSuccess(response);
        }

        /**
         * Tracks one last definitive failure and returns to the caller by notifying the wrapped listener
         */
        void onDefinitiveFailure(Exception exception) {
            trackFailure(exception);
            responseListener.onFailure(this.exception);
        }

        /**
         * Tracks an exception, which caused a retry hence we should not return yet to the caller
         */
        void trackFailure(Exception exception) {
            this.exception = addSuppressedException(this.exception, exception);
        }
    }

    /**
     * Listener that allows to be notified whenever a failure happens. Useful when sniffing is enabled, so that we can sniff on failure.
     * The default implementation is a no-op.
     */
    public static class FailureListener {
        /**
         * Notifies that the host provided as argument has just failed
         */
        public void onFailure(HttpHost host) {

        }
    }

    /**
     * {@code HostTuple} enables the {@linkplain Node}s and {@linkplain AuthCache} to be set together in a thread
     * safe, volatile way.
     */
    static class NodeTuple<T> {
        final T nodes;
        final AuthCache authCache;

        NodeTuple(final T nodes, final AuthCache authCache) {
            this.nodes = nodes;
            this.authCache = authCache;
        }
    }
}
