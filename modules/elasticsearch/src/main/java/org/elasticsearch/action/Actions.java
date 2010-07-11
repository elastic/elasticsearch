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

package org.elasticsearch.action;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;

/**
 * @author kimchy (shay.banon)
 */
public class Actions {

    public static ActionRequestValidationException addValidationError(String error, ActionRequestValidationException validationException) {
        if (validationException == null) {
            validationException = new ActionRequestValidationException();
        }
        validationException.addValidationError(error);
        return validationException;
    }

    public static boolean isAllNodes(String... nodesIds) {
        return nodesIds == null || nodesIds.length == 0 || (nodesIds.length == 1 && nodesIds[0].equals("_all"));
    }

    public static String[] buildNodesIds(DiscoveryNodes nodes, String... nodesIds) {
        if (isAllNodes(nodesIds)) {
            int index = 0;
            nodesIds = new String[nodes.size()];
            for (DiscoveryNode node : nodes) {
                nodesIds[index++] = node.id();
            }
            return nodesIds;
        } else {
            String[] resolvedNodesIds = new String[nodesIds.length];
            for (int i = 0; i < nodesIds.length; i++) {
                if (nodesIds[i].equals("_local")) {
                    resolvedNodesIds[i] = nodes.localNodeId();
                } else if (nodesIds[i].equals("_master")) {
                    resolvedNodesIds[i] = nodes.masterNodeId();
                } else {
                    resolvedNodesIds[i] = nodesIds[i];
                }
            }
            return resolvedNodesIds;
        }
    }
}
