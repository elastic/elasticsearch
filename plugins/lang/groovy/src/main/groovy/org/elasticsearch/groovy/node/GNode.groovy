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

package org.elasticsearch.groovy.node

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.groovy.client.GClient
import org.elasticsearch.node.Node

/**
 * @author kimchy (shay.banon)
 */
class GNode {

    final Node node

    final GClient client

    GNode(Node node) {
        this.node = node
        this.client = new GClient(node.client())
    }

    /**
     * The settings that were used to create the node.
     */
    Settings getSettings() {
        node.settings()
    }

    /**
     * Start the node. If the node is already started, this method is no-op.
     */
    GNode start() {
        node.start()
        this
    }

    /**
     * Stops the node. If the node is already started, this method is no-op.
     */
    GNode stop() {
        node.stop()
        this
    }

    /**
     * Closes the node (and {@link #stop}s if it's running).
     */
    GNode close() {
        node.close()
        this
    }
}
