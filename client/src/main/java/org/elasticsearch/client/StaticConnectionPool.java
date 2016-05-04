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
import org.apache.http.HttpHost;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class StaticConnectionPool extends AbstractStaticConnectionPool {

    private static final Log logger = LogFactory.getLog(StaticConnectionPool.class);

    private final CloseableHttpClient client;
    private final boolean pingEnabled;
    private final RequestConfig pingRequestConfig;
    private final List<StatefulConnection> connections;

    public StaticConnectionPool(CloseableHttpClient client, boolean pingEnabled, RequestConfig pingRequestConfig,
                                Predicate<Connection> connectionSelector, HttpHost... hosts) {
        super(connectionSelector);
        Objects.requireNonNull(client, "client cannot be null");
        Objects.requireNonNull(pingRequestConfig, "pingRequestConfig cannot be null");
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException("no hosts provided");
        }
        this.client = client;
        this.pingEnabled = pingEnabled;
        this.pingRequestConfig = pingRequestConfig;
        this.connections = createConnections(hosts);
    }

    @Override
    protected List<StatefulConnection> getConnections() {
        return connections;
    }

    @Override
    public void beforeAttempt(StatefulConnection connection) throws IOException {
        if (pingEnabled && connection.shouldBeRetried()) {
            HttpHead httpHead = new HttpHead("/");
            httpHead.setConfig(pingRequestConfig);
            StatusLine statusLine;
            try(CloseableHttpResponse httpResponse = client.execute(connection.getHost(), httpHead)) {
                statusLine = httpResponse.getStatusLine();
                EntityUtils.consume(httpResponse.getEntity());
            } catch(IOException e) {
                RequestLogger.log(logger, "ping failed", httpHead.getRequestLine(), connection.getHost(), e);
                onFailure(connection);
                throw e;
            }
            if (statusLine.getStatusCode() >= 300) {
                RequestLogger.log(logger, "ping failed", httpHead.getRequestLine(), connection.getHost(), statusLine);
                onFailure(connection);
                throw new ElasticsearchResponseException(httpHead.getRequestLine(), connection.getHost(), statusLine);
            } else {
                RequestLogger.log(logger, "ping succeeded", httpHead.getRequestLine(), connection.getHost(), statusLine);
                onSuccess(connection);
            }
        }
    }

    @Override
    public void close() throws IOException {
        //no-op nothing to close
    }
}
