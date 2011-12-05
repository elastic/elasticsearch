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

package org.elasticsearch.cluster.block;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.ImmutableSet;

/**
 * @author kimchy (shay.banon)
 */
public class ClusterBlockException extends ElasticSearchException {

    private final ImmutableSet<ClusterBlock> blocks;

    public ClusterBlockException(ImmutableSet<ClusterBlock> blocks) {
        super(buildMessage(blocks));
        this.blocks = blocks;
    }

    public boolean retryable() {
        for (ClusterBlock block : blocks) {
            if (!block.retryable()) {
                return false;
            }
        }
        return true;
    }

    public ImmutableSet<ClusterBlock> blocks() {
        return blocks;
    }

    private static String buildMessage(ImmutableSet<ClusterBlock> blocks) {
        StringBuilder sb = new StringBuilder("blocked by: ");
        for (ClusterBlock block : blocks) {
            sb.append("[").append(block.id()).append("/").append(block.description()).append("];");
        }
        return sb.toString();
    }
}
