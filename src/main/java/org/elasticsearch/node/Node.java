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

package org.elasticsearch.node;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;

/**
 * A node represent a node within a cluster (<tt>cluster.name</tt>). The {@link #client()} can be used
 * in order to use a {@link Client} to perform actions/operations against the cluster.
 * <p/>
 * <p>In order to create a node, the {@link NodeBuilder} can be used. When done with it, make sure to
 * call {@link #close()} on it.
 *
 *
 */
public interface Node extends Releasable{

    /**
     * The settings that were used to create the node.
     */
    Settings settings();

    /**
     * A client that can be used to execute actions (operations) against the cluster.
     */
    Client client();

    /**
     * Start the node. If the node is already started, this method is no-op.
     */
    Node start();

    /**
     * Stops the node. If the node is already stopped, this method is no-op.
     */
    Node stop();

    /**
     * Closes the node (and {@link #stop}s if its running).
     */
    void close();

    /**
     * Returns <tt>true</tt> if the node is closed.
     */
    boolean isClosed();
}
