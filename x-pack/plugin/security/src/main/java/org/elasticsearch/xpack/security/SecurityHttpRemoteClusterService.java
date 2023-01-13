/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.TargetAuthenticationStrategy;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.http.HttpRemoteClusterService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.AccessController;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.net.ssl.SSLContext;

public class SecurityHttpRemoteClusterService implements HttpRemoteClusterService {
    private static final Logger logger = LogManager.getLogger(SecurityHttpRemoteClusterService.class);

    public static final Setting.AffixSetting<List<String>> HTTP_REMOTE_HOST = Setting.affixKeySetting(
        "cluster.http_remote.",
        "host",
        key -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<String> HTTP_REMOTE_AUTHENTICATION = Setting.affixKeySetting(
        "cluster.http_remote.",
        "authorization",
        key -> Setting.simpleString(key, Setting.Property.NodeScope)
    );
    private final Map<String, MinimalRestClient> remoteClusterClients;

    public SecurityHttpRemoteClusterService(Settings settings) {
        final Map<String, Settings> settingsGroups = settings.getGroups("cluster.http_remote");
        final var remoteClusterClients = new HashMap<String, MinimalRestClient>();
        settingsGroups.keySet().forEach(key -> {
            final List<String> hosts = HTTP_REMOTE_HOST.getConcreteSettingForNamespace(key).get(settings);
            final List<HttpHost> httpHosts = hosts.stream().map(HttpHost::new).toList();
            final CloseableHttpAsyncClient httpClient = AccessController.doPrivileged(
                (PrivilegedAction<CloseableHttpAsyncClient>) SecurityHttpRemoteClusterService::createHttpClient
            );
            httpClient.start();
            // TODO: lifecycle management for closing the client
            final List<Header> defaultHeaders = List.of(
                new BasicHeader("Authorization", HTTP_REMOTE_AUTHENTICATION.getConcreteSettingForNamespace(key).get(settings))
            );
            remoteClusterClients.put(key, new MinimalRestClient(httpClient, httpHosts, defaultHeaders));
        });
        this.remoteClusterClients = Map.copyOf(remoteClusterClients);
        logger.info("Initialised remote cluster clients [{}]", remoteClusterClients);
    }

    @Override
    public boolean isHttpRemoteClusterAlias(String clusterAlias) {
        return remoteClusterClients.containsKey(clusterAlias);
    }

    @Override
    public void relayRequest(String clusterAlias, String action, TransportRequest transportRequest, ActionListener<byte[]> listener) {
        final MinimalRestClient restClient = remoteClusterClients.get(clusterAlias);
        if (restClient == null) {
            throw new IllegalStateException("no http remote cluster client found for [" + clusterAlias + "]");
        }
        doRelayRequest(restClient, action, transportRequest, listener);
    }

    public static void doRelayRequest(
        MinimalRestClient restClient,
        String action,
        TransportRequest transportRequest,
        ActionListener<byte[]> listener
    ) {
        final BytesStreamOutput out = new BytesStreamOutput();
        try {
            transportRequest.writeTo(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        final String payload = Base64.getEncoder().encodeToString(out.bytes().array());

        final HttpPost httpPost = new HttpPost(restClient.httpHosts.get(0).toURI() + "/_transport_relay");
        httpPost.setHeaders(restClient.defaultHeaders.toArray(Header[]::new));
        httpPost.setEntity(
            new StringEntity("{\"action\":\"%s\",\"payload\":\"%s\"}".formatted(action, payload), ContentType.APPLICATION_JSON)
        );
        // TODO: extract user's remote privileges
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            restClient.client.execute(httpPost, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse result) {
                    try {
                        final Map<String, Object> m = XContentHelper.convertToMap(
                            JsonXContent.jsonXContent,
                            result.getEntity().getContent(),
                            false
                        );
                        final String body = (String) m.get("body");
                        if (body == null) {
                            throw new IllegalStateException("body not found in response [" + m + "]");
                        }
                        listener.onResponse(Base64.getDecoder().decode(body));
                    } catch (Exception e) {
                        failed(e);
                    }
                }

                @Override
                public void failed(Exception ex) {
                    listener.onFailure(ex);
                }

                @Override
                public void cancelled() {
                    listener.onFailure(new RuntimeException("cancelled"));
                }
            });
            return null;
        });
    }

    public static CloseableHttpAsyncClient createHttpClient() {
        // default timeouts are all infinite
        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom().setConnectTimeout(1000).setSocketTimeout(30000);

        try {
            HttpAsyncClientBuilder httpClientBuilder = HttpAsyncClientBuilder.create()
                .setDefaultRequestConfig(requestConfigBuilder.build())
                // default settings for connection pooling may be too constraining
                .setMaxConnPerRoute(10)
                .setMaxConnTotal(30)
                .setSSLContext(SSLContext.getDefault())
                .setTargetAuthenticationStrategy(new TargetAuthenticationStrategy());
            final HttpAsyncClientBuilder finalBuilder = httpClientBuilder;
            return AccessController.doPrivileged((PrivilegedAction<CloseableHttpAsyncClient>) finalBuilder::build);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("could not create the default ssl context", e);
        }
    }

    public static class MinimalRestClient {
        final CloseableHttpAsyncClient client;
        final List<HttpHost> httpHosts;
        final List<Header> defaultHeaders;

        public MinimalRestClient(CloseableHttpAsyncClient client, List<HttpHost> httpHosts, List<Header> defaultHeaders) {
            this.client = client;
            this.httpHosts = httpHosts;
            this.defaultHeaders = defaultHeaders;
        }

        @Override
        public String toString() {
            return "MinimalRestClient{" + "client=" + client + ", httpHosts=" + httpHosts + ", defaultHeaders=" + defaultHeaders + '}';
        }

        public CloseableHttpAsyncClient getClient() {
            return client;
        }
    }
}
