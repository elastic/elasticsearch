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

package org.elasticsearch.wildfly.transport;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.enterprise.inject.Produces;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;

@SuppressWarnings("unused")
public final class TransportClientProducer {

    @Produces
    public TransportClient createTransportClient() throws IOException {
        final String elasticsearchProperties = System.getProperty("elasticsearch.properties");
        final Properties properties = new Properties();

        final String transportUri;
        final String clusterName;
        try (InputStream is = Files.newInputStream(getPath(elasticsearchProperties))) {
            properties.load(is);
            transportUri = properties.getProperty("transport.uri");
            clusterName = properties.getProperty("cluster.name");
        }

        final int lastColon = transportUri.lastIndexOf(':');
        final String host = transportUri.substring(0, lastColon);
        final int port = Integer.parseInt(transportUri.substring(lastColon + 1));
        final Settings settings = Settings.builder().put("cluster.name", clusterName).build();
        final TransportClient transportClient = new PreBuiltTransportClient(settings, Collections.emptyList());
        transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName(host), port));
        return transportClient;
    }

    @SuppressForbidden(reason = "get path not configured in environment")
    private Path getPath(final String elasticsearchProperties) {
        return PathUtils.get(elasticsearchProperties);
    }

}
