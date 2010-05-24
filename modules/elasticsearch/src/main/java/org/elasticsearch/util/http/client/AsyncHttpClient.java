/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */
package org.elasticsearch.util.http.client;

import org.elasticsearch.util.collect.Multimap;
import org.elasticsearch.util.http.client.Request.EntityWriter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * This class support asynchronous and synchronous HTTP request.
 *
 * To execute synchronous HTTP request, you just need to do
 * {@code
 * AsyncHttpClient c = new AsyncHttpClient();
 * Future<Response> f = c.prepareGet("http://www.ning.com/").execute();
 * }
 *
 * The code above will block until the response is fully received. To execute asynchronous HTTP request, you
 * create an {@link AsyncHandler} or its abstract implementation, {@link org.elasticsearch.util.http.client.AsyncCompletionHandler}
 *
 * {@code
 * AsyncHttpClient c = new AsyncHttpClient();
 * Future<Response> f = c.prepareGet("http://www.ning.com/").execute(new AsyncCompletionHandler<Response>() &#123;
 *
 * &#64;Override
 * public Response onCompleted(Response response) throws IOException &#123;
 * // Do something
 * return response;
 * &#125;
 *
 * &#64;Override
 * public void onThrowable(Throwable t) &#123;
 * &#125;
 * &#125;);
 * Response response = f.get();
 *
 * // We are just interested to retrieve the status code.
 * Future<Integer> f = c.prepareGet("http://www.ning.com/").execute(new AsyncCompletionHandler<Integer>() &#123;
 *
 * &#64;Override
 * public Integer onCompleted(Response response) throws IOException &#123;
 * // Do something
 * return response.getStatusCode();
 * &#125;
 *
 * &#64;Override
 * public void onThrowable(Throwable t) &#123;
 * &#125;
 * &#125;);
 * Integer statusCode = f.get();
 * }
 * The {@link AsyncCompletionHandler#onCompleted(org.elasticsearch.util.http.client.Response)} will be invoked once the http response has been fully read, which include
 * the http headers and the response body. Note that the entire response will be buffered in memory.
 *
 * You can also have more control about the how the response is asynchronously processed by using a {@link AsyncHandler}
 * {@code
 * AsyncHttpClient c = new AsyncHttpClient();
 * Future<String> f = c.prepareGet("http://www.ning.com/").execute(new AsyncHandler<String>() &#123;
 * private StringBuilder builder = new StringBuilder();
 *
 * &#64;Override
 * public void onStatusReceived(HttpResponseStatus s) throws Exception &#123;
 * // The Status have been read
 * // If you don't want to read the headers,body, or stop processing the response
 * throw new ResponseComplete();
 * }
 *
 * &#64;Override
 * public void onHeadersReceived(HttpResponseHeaders bodyPart) throws Exception &#123;
 * // The headers have been read
 * // If you don't want to read the body, or stop processing the response
 * throw new ResponseComplete();
 * }
 * &#64;Override
 *
 * public void onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception &#123;
 * builder.append(new String(bodyPart));
 * &#125;
 *
 * &#64;Override
 * public String onCompleted() throws Exception &#123;
 * // Will be invoked once the response has been fully read or a ResponseComplete exception
 * // has been thrown.
 * return builder.toString();
 * &#125;
 *
 * &#64;Override
 * public void onThrowable(Throwable t) &#123;
 * &#125;
 * &#125;);
 *
 * String bodyResponse = f.get();
 * }
 * From any {@link HttpContent} sub classses, you can asynchronously process the response status,headers and body and decide when to
 * stop the processing the response by throwing a new {link ResponseComplete} at any moment.
 *
 * This class can also be used without the need of {@link AsyncHandler}</p>
 * {@code
 * AsyncHttpClient c = new AsyncHttpClient();
 * Future<Response> f = c.prepareGet(TARGET_URL).execute();
 * Response r = f.get();
 * }
 *
 * Finally, you can configure the AsyncHttpClient using an {@link AsyncHttpClientConfig} instance</p>
 * {@code
 * AsyncHttpClient c = new AsyncHttpClient(new AsyncHttpClientConfig.Builder().setRequestTimeoutInMs(...).build());
 * Future<Response> f = c.prepareGet(TARGET_URL).execute();
 * Response r = f.get();
 * }
 *
 * An instance of this class will cache every HTTP 1.1 connections and close them when the {@link AsyncHttpClientConfig#getIdleConnectionTimeoutInMs()}
 * expires. This object can hold many persistent connections to different host.
 */
public class AsyncHttpClient {

    private final static String DEFAULT_PROVIDER = "org.elasticsearch.util.http.client.providers.NettyAsyncHttpProvider";
    private final AsyncHttpProvider<?> httpProvider;
    private final AsyncHttpClientConfig config;

    /**
     * Create a new HTTP Asynchronous Client using the default {@link AsyncHttpClientConfig} configuration. The
     * default {@link AsyncHttpProvider} will be used ({@link org.elasticsearch.util.http.client.providers.NettyAsyncHttpProvider}
     */
    public AsyncHttpClient() {
        this(new AsyncHttpClientConfig.Builder().build());
    }

    /**
     * Create a new HTTP Asynchronous Client using an implementation of {@link AsyncHttpProvider} and
     * the default {@link AsyncHttpClientConfig} configuration.
     *
     * @param provider a {@link AsyncHttpProvider}
     */
    public AsyncHttpClient(AsyncHttpProvider<?> provider) {
        this(provider, new AsyncHttpClientConfig.Builder().build());
    }

    /**
     * Create a new HTTP Asynchronous Client using a {@link AsyncHttpClientConfig} configuration and the
     * {@link #DEFAULT_PROVIDER}
     *
     * @param config a {@link AsyncHttpClientConfig}
     */
    public AsyncHttpClient(AsyncHttpClientConfig config) {
        this(loadDefaultProvider(DEFAULT_PROVIDER, config), config);
    }

    /**
     * Create a new HTTP Asynchronous Client using a {@link AsyncHttpClientConfig} configuration and
     * and a {@link AsyncHttpProvider}.
     *
     * @param config       a {@link AsyncHttpClientConfig}
     * @param httpProvider a {@link AsyncHttpProvider}
     */
    public AsyncHttpClient(AsyncHttpProvider<?> httpProvider, AsyncHttpClientConfig config) {
        this.config = config;
        this.httpProvider = httpProvider;
    }

    /**
     * Create a new HTTP Asynchronous Client using a {@link AsyncHttpClientConfig} configuration and
     * and a AsyncHttpProvider class' name.
     *
     * @param config        a {@link AsyncHttpClientConfig}
     * @param providerClass a {@link AsyncHttpProvider}
     */
    public AsyncHttpClient(String providerClass, AsyncHttpClientConfig config) {
        this.config = new AsyncHttpClientConfig.Builder().build();
        this.httpProvider = loadDefaultProvider(providerClass, config);
    }

    public class BoundRequestBuilder extends RequestBuilderBase<BoundRequestBuilder> {
        private BoundRequestBuilder(RequestType type) {
            super(BoundRequestBuilder.class, type);
        }

        private BoundRequestBuilder(Request prototype) {
            super(BoundRequestBuilder.class, prototype);
        }

        public <T> Future<T> execute(AsyncHandler<T> handler) throws IOException {
            return AsyncHttpClient.this.executeRequest(build(), handler);
        }

        public Future<Response> execute() throws IOException {
            return AsyncHttpClient.this.executeRequest(build(), new AsyncCompletionHandlerBase());
        }

        // Note: For now we keep the delegates in place even though they are not needed
        //       since otherwise Clojure (and maybe other languages) won't be able to
        //       access these methods - see Clojure tickets 126 and 259

        @Override
        public BoundRequestBuilder addBodyPart(Part part) throws IllegalArgumentException {
            return super.addBodyPart(part);
        }

        @Override
        public BoundRequestBuilder addCookie(Cookie cookie) {
            return super.addCookie(cookie);
        }

        @Override
        public BoundRequestBuilder addHeader(String name, String value) {
            return super.addHeader(name, value);
        }

        @Override
        public BoundRequestBuilder addParameter(String key, String value) throws IllegalArgumentException {
            return super.addParameter(key, value);
        }

        @Override
        public BoundRequestBuilder addQueryParameter(String name, String value) {
            return super.addQueryParameter(name, value);
        }

        @Override
        public Request build() {
            return super.build();
        }

        @Override
        public BoundRequestBuilder setBody(byte[] data) throws IllegalArgumentException {
            return super.setBody(data);
        }

        @Override
        public BoundRequestBuilder setBody(EntityWriter dataWriter, long length) throws IllegalArgumentException {
            return super.setBody(dataWriter, length);
        }

        @Override
        public BoundRequestBuilder setBody(EntityWriter dataWriter) {
            return super.setBody(dataWriter);
        }

        @Override
        public BoundRequestBuilder setBody(InputStream stream) throws IllegalArgumentException {
            return super.setBody(stream);
        }

        @Override
        public BoundRequestBuilder setBody(String data) throws IllegalArgumentException {
            return super.setBody(data);
        }

        @Override
        public BoundRequestBuilder setHeader(String name, String value) {
            return super.setHeader(name, value);
        }

        @Override
        public BoundRequestBuilder setHeaders(Headers headers) {
            return super.setHeaders(headers);
        }

        @Override
        public BoundRequestBuilder setParameters(Map<String, String> parameters) throws IllegalArgumentException {
            return super.setParameters(parameters);
        }

        @Override
        public BoundRequestBuilder setParameters(Multimap<String, String> parameters) throws IllegalArgumentException {
            return super.setParameters(parameters);
        }

        @Override
        public BoundRequestBuilder setUrl(String url) {
            return super.setUrl(url);
        }

        @Override
        public BoundRequestBuilder setVirtualHost(String virtualHost) {
            return super.setVirtualHost(virtualHost);
        }
    }

    /**
     * Return the asynchronouys {@link org.elasticsearch.util.http.client.AsyncHttpProvider}
     *
     * @return an {@link org.elasticsearch.util.http.client.AsyncHttpProvider}
     */
    public AsyncHttpProvider<?> getProvider() {
        return httpProvider;
    }

    /**
     * Close the underlying connections.
     */
    public void close() {
        httpProvider.close();
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    /**
     * Return the {@link org.elasticsearch.util.http.client.AsyncHttpClientConfig}
     *
     * @return {@link org.elasticsearch.util.http.client.AsyncHttpClientConfig}
     */
    public AsyncHttpClientConfig getConfig() {
        return config;
    }

    /**
     * Prepare an HTTP client GET request.
     *
     * @param url A well formed URL.
     * @return {@link RequestBuilder}
     */
    public BoundRequestBuilder prepareGet(String url) {
        return new BoundRequestBuilder(RequestType.GET).setUrl(url);
    }

    /**
     * Prepare an HTTP client HEAD request.
     *
     * @param url A well formed URL.
     * @return {@link RequestBuilder}
     */
    public BoundRequestBuilder prepareHead(String url) {
        return new BoundRequestBuilder(RequestType.HEAD).setUrl(url);
    }

    /**
     * Prepare an HTTP client POST request.
     *
     * @param url A well formed URL.
     * @return {@link RequestBuilder}
     */
    public BoundRequestBuilder preparePost(String url) {
        return new BoundRequestBuilder(RequestType.POST).setUrl(url);
    }

    /**
     * Prepare an HTTP client PUT request.
     *
     * @param url A well formed URL.
     * @return {@link RequestBuilder}
     */
    public BoundRequestBuilder preparePut(String url) {
        return new BoundRequestBuilder(RequestType.PUT).setUrl(url);
    }

    /**
     * Prepare an HTTP client DELETE request.
     *
     * @param url A well formed URL.
     * @return {@link RequestBuilder}
     */
    public BoundRequestBuilder prepareDelete(String url) {
        return new BoundRequestBuilder(RequestType.DELETE).setUrl(url);
    }

    /**
     * Construct a {@link RequestBuilder} using a {@link Request}
     *
     * @param request a {@link Request}
     * @return {@link RequestBuilder}
     */
    public BoundRequestBuilder prepareRequest(Request request) {
        return new BoundRequestBuilder(request);
    }

    /**
     * Execute an HTTP request.
     *
     * @param request {@link Request}
     * @param handler an instance of {@link AsyncHandler}
     * @param <T>     Type of the value that will be returned by the associated {@link java.util.concurrent.Future}
     * @return a {@link Future} of type T
     * @throws IOException
     */
    public <T> Future<T> executeRequest(Request request, AsyncHandler<T> handler) throws IOException {
        return httpProvider.execute(request, handler);
    }

    /**
     * Execute an HTTP request.
     *
     * @param request {@link Request}
     * @return a {@link Future} of type Response
     * @throws IOException
     */
    public Future<Response> executeRequest(Request request) throws IOException {
        return httpProvider.execute(request, new AsyncCompletionHandlerBase());
    }

    @SuppressWarnings("unchecked")
    private final static AsyncHttpProvider<?> loadDefaultProvider(String className, AsyncHttpClientConfig config) {
        try {
            Class<AsyncHttpProvider<?>> providerClass = (Class<AsyncHttpProvider<?>>) Thread.currentThread()
                    .getContextClassLoader().loadClass(className);
            return (AsyncHttpProvider<?>) providerClass.getDeclaredConstructor(
                    new Class[]{AsyncHttpClientConfig.class}).newInstance(new Object[]{config});
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
