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

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

/**
 */
public class RestClient implements Closeable{

    private final CloseableHttpClient client;
    private volatile Set<HttpHost> hosts;
    private final String scheme;
    private final Set<HttpHost> blackList = new CopyOnWriteArraySet<>();

    public RestClient(HttpHost... hosts) {
        this("http", HttpClientBuilder.create().setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(100).build()).build(), hosts);
    }

    public RestClient(String scheme, CloseableHttpClient client, HttpHost[] hosts) {
        if (hosts.length == 0) {
            throw new IllegalArgumentException("hosts must note be empty");
        }
        this.scheme = scheme;
        this.client = client;
        this.hosts = new HashSet<>(Arrays.asList(hosts));
    }


    public HttpResponse httpGet(String endpoint, Map<String, Object> params) throws IOException {
        return httpGet(getHostIterator(true), endpoint, params);
    }

    HttpResponse httpGet(Iterable<HttpHost> hosts, String endpoint, Map<String, Object> params) throws IOException {
        HttpUriRequest request = new HttpGet(buildUri(endpoint, pairs(params)));
        return execute(request, hosts);
    }

    HttpResponse httpDelete(String endpoint, Map<String, Object> params) throws IOException {
        HttpUriRequest request = new HttpDelete(buildUri(endpoint, pairs(params)));
        return execute(request, getHostIterator(true));
    }

    HttpResponse httpPut(String endpoint, HttpEntity body, Map<String, Object>  params) throws IOException {
        HttpPut request = new HttpPut(buildUri(endpoint, pairs(params)));
        request.setEntity(body);
        return execute(request, getHostIterator(true));
    }

    HttpResponse httpPost(String endpoint, HttpEntity body, Map<String, Object> params) throws IOException {
        HttpPost request = new HttpPost(buildUri(endpoint, pairs(params)));
        request.setEntity(body);
        return execute(request, getHostIterator(true));
    }

    private List<NameValuePair> pairs(Map<String, Object> options) {
        return options.entrySet().stream().map(e -> new BasicNameValuePair(e.getKey(), e.getValue().toString()))
            .collect(Collectors.toList());
    }

    public HttpResponse execute(HttpUriRequest request, Iterable<HttpHost> retryHosts) throws IOException {
        IOException exception = null;
        for (HttpHost singleHost : retryHosts) {
            try {
                return client.execute(singleHost, request);
            } catch (IOException ex) {
                if (this.hosts.contains(singleHost)) {
                    blackList.add(singleHost);
                }
                if (exception != null) {
                    exception.addSuppressed(ex);
                } else {
                    exception = ex;
                }
            }
        }
        throw exception;
    }

    public URI buildUri(String path, List<NameValuePair> query) {
        try {
            return new URI(null, null, null, -1, path, URLEncodedUtils.format(query, StandardCharsets.UTF_8), null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public Set<HttpHost> fetchNodes(HttpHost host, boolean useClientNodes, boolean local, boolean checkAvailable) throws IOException {
        HttpResponse httpResponse = httpGet(Collections.singleton(host), "/_cat/nodes", Collections.singletonMap("h", "http,role"));
        StatusLine statusLine = httpResponse.getStatusLine();
        if (statusLine.getStatusCode() != 200) {
            throw new RuntimeException("failed to fetch nodes: " + statusLine.getReasonPhrase());
        }
        HttpEntity entity = httpResponse.getEntity();
        Set<HttpHost> hosts = new HashSet<>();
        try (BufferedReader content = new BufferedReader(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {
            String line;
            while((line = content.readLine()) != null) {
                final String[] split = line.split("\\s+");
                assert split.length == 2;
                String boundAddress = split[0];
                String role = split[1];
                if ("-".equals(split[0].trim()) == false) {
                    if ("d".equals(role.trim()) == false && useClientNodes == false) {
                        continue;
                    }
                    URI boundAddressAsURI = URI.create("http://" + boundAddress);
                    HttpHost newHost = new HttpHost(boundAddressAsURI.getHost(), boundAddressAsURI.getPort(), scheme);
                    if (checkAvailable == false || isAvailable(newHost)) {
                        hosts.add(newHost);
                    }
                }
            }
        }
        return hosts;
    }

    public String getClusterName(HttpHost host) throws IOException {
        HttpResponse httpResponse = httpGet(Collections.singleton(host), "/_cat/health", Collections.singletonMap("h", "cluster"));
        StatusLine statusLine = httpResponse.getStatusLine();
        if (statusLine.getStatusCode() != 200) {
            throw new RuntimeException("failed to fetch nodes: " + statusLine.getReasonPhrase());
        }
        HttpEntity entity = httpResponse.getEntity();
        try (BufferedReader content = new BufferedReader(new InputStreamReader(entity.getContent(), StandardCharsets.UTF_8))) {
            String clusterName = content.readLine().trim();
            if (clusterName.length() == 0) {
                throw new IllegalStateException("clustername must not be empty");
            }
            return clusterName;
        }
    }

    public boolean isAvailable(HttpHost host) {
        try {
            HttpResponse httpResponse = httpGet(Collections.singleton(host), "/", Collections.emptyMap());
            StatusLine statusLine = httpResponse.getStatusLine();
            return statusLine.getStatusCode() == 200;
        } catch (IOException ex) {
            return false;
        }
    }

    public synchronized void setNodes(Set<HttpHost> hosts) {
        this.hosts = Collections.unmodifiableSet(new HashSet<>(hosts));
        blackList.retainAll(hosts);
    }

    public Set<HttpHost> getHosts() {
        return hosts;
    }

    protected Iterable<HttpHost> getHostIterator(boolean clearBlacklist) {
        if (hosts.size() == blackList.size() && clearBlacklist) {
            blackList.clear(); // lets try again
        }
        return () -> hosts.stream().filter((h) -> blackList.contains(h) == false).iterator();
    }

    int getNumHosts() {
        return hosts.size();
    }

    int getNumBlacklistedHosts() {
        return blackList.size();
    }
    @Override
    public void close() throws IOException {
        client.close();
    }
}
