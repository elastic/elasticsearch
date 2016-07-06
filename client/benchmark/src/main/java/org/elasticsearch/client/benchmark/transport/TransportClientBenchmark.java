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
package org.elasticsearch.client.benchmark.transport;

import org.elasticsearch.client.benchmark.ClientBenchmark;
import org.elasticsearch.client.benchmark.ops.bulk.BulkRequestExecutor;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;

import java.net.InetAddress;

public final class TransportClientBenchmark extends ClientBenchmark {
    @Override
    protected BulkRequestExecutor requestExecutor(String hostName, String indexName, String typeName) throws Exception {
        Settings clientSettings = Settings.builder()
//            .put("node.name", "benchmark-client")
//            .put("client.transport.ignore_cluster_name", true)
//            .put(Environment.PATH_HOME_SETTING.getKey(), "/tmp")
            .put(Node.NODE_MODE_SETTING.getKey(), "network").build();

        TransportClient client = TransportClient.builder().settings(clientSettings).build();
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), 9300));
        return new TransportBulkRequestExecutor(client, indexName, typeName);
    }

    public static void main(String[] args) throws Exception {
        TransportClientBenchmark benchmark = new TransportClientBenchmark();
        benchmark.run(args);
    }
}
