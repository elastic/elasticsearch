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
package org.elasticsearch.test.rest.client.http;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.rest.RestStatus;

import java.io.*;
import java.net.*;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class HttpClient {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    private final URI baseUrl;
    private String path;
    private String method;
    private Map<String, String> headers;
    private Map<String, String> params;
    private String payload;

    public static HttpClient instance(String hostname, Integer port) {
        return new HttpClient("http", hostname, port);
    }

    public static HttpClient instance(HttpServerTransport transport) {
        InetSocketTransportAddress transportAddress = (InetSocketTransportAddress) transport.boundAddress().publishAddress();
        return new HttpClient("http", transportAddress.address().getHostName(), transportAddress.address().getPort());
   }

    public static HttpClient instance(String protocol, String hostname, Integer port) {
        return new HttpClient(protocol, hostname, port);
    }

    private HttpClient(String protocol, String hostname, Integer port) {
        try {
            // Hack because HttpURLConnection silently ignore Origin header
            // http://stackoverflow.com/questions/11147330/httpurlconnection-wont-let-me-set-via-header
            System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
            baseUrl = new URI(protocol, null, hostname, port, null, null, null);
        } catch (URISyntaxException e) {
            throw new ElasticsearchException("URL is not well formed", e);
        }
        reset();
    }

    public void reset() {
        path = "/";
        method = "GET";
        headers = Maps.newHashMap();
        params = Maps.newHashMap();
        payload = null;
    }

    public HttpClient path(String path) {
        this.path = path;
        return this;
    }

    public HttpClient method(String method) {
        this.method = method;
        return this;
    }

    public HttpClient addParam(String key, String value) {
        this.params.put(key, value);
        return this;
    }

    public HttpClient addHeader(String key, String value) {
        this.headers.put(key, value);
        return this;
    }

    public HttpClient addHeaders(Map<String, String> headers) {
        this.headers.putAll(headers);
        return this;
    }

    public HttpClient payload(String payload) {
        this.payload = payload;
        return this;
    }


    static String urlEncodeUTF8(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new UnsupportedOperationException(e);
        }
    }
    static String urlEncodeUTF8(Map<?,?> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<?,?> entry : map.entrySet()) {
            if (sb.length() > 0) {
                sb.append("&");
            }
            sb.append(String.format(Locale.getDefault(), "%s=%s",
                    urlEncodeUTF8(entry.getKey().toString()),
                    urlEncodeUTF8(entry.getValue().toString())
            ));
        }
        return sb.toString();
    }

    public HttpResponse execute() {
        URL url;

        StringBuilder completePath = new StringBuilder(path);

        List<Integer> ignores = Lists.newArrayList();
        ignores.add(RestStatus.MOVED_PERMANENTLY.getStatus());
        if (params != null && params.isEmpty() == false) {
            //makes a copy of the parameters before modifying them for this specific request
            Map<String, String> requestParams = Maps.newHashMap(params);
            //ignore is a special parameter supported by the clients, shouldn't be sent to es
            String ignoreString = requestParams.remove("ignore");
            if (Strings.hasLength(ignoreString)) {
                try {
                    ignores.add(Integer.valueOf(ignoreString));
                } catch(NumberFormatException e) {
                    throw new IllegalArgumentException("ignore value should be a number, found [" + ignoreString + "] instead");
                }
            }

            if (requestParams.size() > 0) {
                // We URLEncode all parameters
                completePath.append("?").append(urlEncodeUTF8(requestParams));
            }
        }

        try {
            URI resolve = baseUrl.resolve(completePath.toString());
            url = new URL(resolve.toASCIIString());
        } catch (MalformedURLException e) {
            throw new ElasticsearchException("Cannot create URL from " + baseUrl + " " + completePath, e);
        }

        logger.debug("---> EXECUTING HTTP CALL [{}] [{}]", method, url);

        HttpURLConnection urlConnection;
        try {
            urlConnection = (HttpURLConnection) url.openConnection();
            urlConnection.setRequestProperty("Accept-Charset", Charsets.UTF_8.name());
            urlConnection.setRequestMethod(method);
            urlConnection.setInstanceFollowRedirects(false);
            for (Map.Entry<String, String> headerEntry : headers.entrySet()) {
                urlConnection.setRequestProperty(headerEntry.getKey(), headerEntry.getValue());
            }

            if (Strings.hasText(payload)) {
                logger.trace("payload [{}]", payload);
                urlConnection.setDoOutput(true);
                urlConnection.setRequestProperty("Content-Type", "application/json");
                urlConnection.setRequestProperty("Accept", "application/json");

                // If we try to do DELETE, Java < 8 does not allow it (http://bugs.java.com/view_bug.do?bug_id=7157360).
                // Here is a hack. TODO Remove when JDK8 will be mandatory
                if (method.equals("DELETE")) {
                    urlConnection.setRequestMethod("POST");
                    urlConnection.setRequestProperty("X-HTTP-Method-Override", "DELETE");
                }

                OutputStreamWriter osw = new OutputStreamWriter(urlConnection.getOutputStream(), Charsets.UTF_8);
                osw.write(payload);
                osw.flush();
                osw.close();
            }

            urlConnection.connect();
        } catch (IOException e) {
            throw new ElasticsearchException("", e);
        }

        int errorCode = -1;
        String reasonPhrase = null;

        Map<String, List<String>> respHeaders = null;
        try {
            errorCode = urlConnection.getResponseCode();
            reasonPhrase = urlConnection.getResponseMessage();
            respHeaders = urlConnection.getHeaderFields();

            String body = null;
            //http HEAD doesn't support response body
            // For the few api (exists class of api) that use it we need to accept 404 too
            if (method.equals("HEAD")) {
                ignores.add(RestStatus.BAD_REQUEST.getStatus());
                ignores.add(RestStatus.NOT_FOUND.getStatus());
            } else {
                InputStream inputStream = urlConnection.getInputStream();
                try {
                    body = Streams.copyToString(new InputStreamReader(inputStream, Charsets.UTF_8));
                } catch (IOException e1) {
                    throw new ElasticsearchException("problem reading error stream", e1);
                }
                logger.debug("---> HTTP RESPONSE [{}] [{}]", errorCode, reasonPhrase, respHeaders, body);
            }

            HttpResponse response = new HttpResponse(method, body, errorCode, reasonPhrase, respHeaders, null);
            checkStatusCode(response, ignores);

            return response;
        } catch (IOException e) {
            InputStream errStream = urlConnection.getErrorStream();
            String body = null;
            if (errStream != null) {
                try {
                    body = Streams.copyToString(new InputStreamReader(errStream, Charsets.UTF_8));
                } catch (IOException e1) {
                    throw new ElasticsearchException("problem reading error stream", e1);
                }
            }
            return new HttpResponse(method, body, errorCode, reasonPhrase, respHeaders, e);
        } finally {
            urlConnection.disconnect();
        }
    }

    private void checkStatusCode(HttpResponse restResponse, List<Integer> ignores) {
        //ignore is a catch within the client, to prevent the client from throwing error if it gets non ok codes back
        if (ignores.contains(restResponse.getStatusCode())) {
            if (logger.isDebugEnabled()) {
                logger.debug("ignored non ok status codes {} as requested", ignores);
            }
            return;
        }
        if (restResponse.isError()) {
            throw new ElasticsearchException("non ok status code [" + restResponse.getStatusCode() + "] returned");
        }
    }
}
