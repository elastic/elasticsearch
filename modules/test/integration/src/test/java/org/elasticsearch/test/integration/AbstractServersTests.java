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
import org.elasticsearch.server.Server;
import org.elasticsearch.util.logging.Loggers;
import org.elasticsearch.util.settings.Settings;
import org.slf4j.Logger;

import java.util.Map;

import static com.google.common.collect.Maps.*;
import static org.elasticsearch.server.ServerBuilder.*;
import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;
import static org.elasticsearch.util.settings.ImmutableSettings.*;

public abstract class AbstractServersTests {

    protected final Logger logger = Loggers.getLogger(getClass());

    private Map<String, Server> servers = newHashMap();

    private Map<String, Client> clients = newHashMap();

    public Server startServer(String id) {
        return buildServer(id).start();
    }

    public Server startServer(String id, Settings settings) {
        return buildServer(id, settings).start();
    }

    public Server buildServer(String id) {
        return buildServer(id, EMPTY_SETTINGS);
    }

    public Server buildServer(String id, Settings settings) {
        String settingsSource = getClass().getName().replace('.', '/') + ".yml";
        Settings finalSettings = settingsBuilder()
                .loadFromClasspath(settingsSource)
                .putAll(settings)
                .put("name", id)
                .build();
        Server server = serverBuilder()
                .settings(finalSettings)
                .build();
        servers.put(id, server);
        clients.put(id, server.client());
        return server;
    }

    public void closeServer(String id) {
        Client client = clients.remove(id);
        if (client != null) {
            client.close();
        }
        Server server = servers.remove(id);
        if (server != null) {
            server.close();
        }
    }

    public Server server(String id) {
        return servers.get(id);
    }

    public Client client(String id) {
        return clients.get(id);
    }

    public void closeAllServers() {
        for (Client client : clients.values()) {
            client.close();
        }
        clients.clear();
        for (Server server : servers.values()) {
            server.close();
        }
        servers.clear();
    }
}