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

package org.elasticsearch.server;

import org.elasticsearch.client.Client;
import org.elasticsearch.util.settings.Settings;

/**
 * A server represent a node within a cluster (<tt>cluster.name</tt>). The {@link #client()} can be used
 * in order to use a {@link Client} to perform actions/operations against the cluster.
 *
 * <p>In order to create a server, the {@link ServerBuilder} can be used. When done with it, make sure to
 * call {@link #close()} on it.
 *
 * @author kimchy (Shay Banon)
 */
public interface Server {

    /**
     * The settings that were used to create the server.
     */
    Settings settings();

    /**
     * A client that can be used to execute actions (operations) against the cluster.
     */
    Client client();

    /**
     * Start the server. If the server is already started, this method is noop.
     */
    Server start();

    /**
     * Stops the server. If the server is already started, this method is noop.
     */
    Server stop();

    /**
     * Closes the server (and {@link #stop}s if its running).
     */
    void close();
}
