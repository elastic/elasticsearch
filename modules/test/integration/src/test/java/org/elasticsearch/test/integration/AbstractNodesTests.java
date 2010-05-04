/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.util.logging.ESLogger;
import org.elasticsearch.util.logging.Loggers;
import org.elasticsearch.util.settings.Settings;

import java.util.Map;

import static org.elasticsearch.node.NodeBuilder.*;
import static org.elasticsearch.util.collect.Maps.*;
import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.util.settings.ImmutableSettings.*;

public abstract class AbstractNodesTests {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    private Map<String, Node> nodes = newHashMap();

    private Map<String, Client> clients = newHashMap();

    public Node startNode(String id) {
        return buildNode(id).start();
    }

    public Node startNode(String id, Settings settings) {
        return buildNode(id, settings).start();
    }

    public Node buildNode(String id) {
        return buildNode(id, EMPTY_SETTINGS);
    }

    public Node buildNode(String id, Settings settings) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .put(settings)
                .put("name", id)
                .build();
        Node node = nodeBuilder()
                .settings(finalSettings)
                .build();
        nodes.put(id, node);
        clients.put(id, node.client());
        return node;
    }

    public void closeNode(String id) {
        Client client = clients.remove(id);
        if (client != null) {
            client.close();
        }
        Node node = nodes.remove(id);
        if (node != null) {
            node.close();
        }
    }

    public Node node(String id) {
        return nodes.get(id);
    }

    public Client client(String id) {
        return clients.get(id);
    }

    public void closeAllNodes() {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Node node : nodes.values()) {
            node.close();
        }
        nodes.clear();
    }
}