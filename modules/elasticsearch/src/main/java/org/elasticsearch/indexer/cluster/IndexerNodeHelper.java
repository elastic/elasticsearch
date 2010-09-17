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

package org.elasticsearch.indexer.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.indexer.IndexerName;

/**
 * @author kimchy (shay.banon)
 */
public class IndexerNodeHelper {

    public static boolean isIndexerNode(DiscoveryNode node) {
        if (node.clientNode()) {
            return false;
        }
        String indexer = node.attributes().get("indexer");
        // by default, if not set, its an indexer node (better OOB exp)
        if (indexer == null) {
            return true;
        }
        if ("_none_".equals(indexer)) {
            return false;
        }
        // there is at least one indexer settings, we need it
        return true;
    }

    public static boolean isIndexerNode(DiscoveryNode node, IndexerName indexerName) {
        if (!isIndexerNode(node)) {
            return false;
        }
        String indexer = node.attributes().get("indexer");
        // by default, if not set, its an indexer node (better OOB exp)
        return indexer == null || indexer.contains(indexerName.type()) || indexer.contains(indexerName.name());
    }
}
