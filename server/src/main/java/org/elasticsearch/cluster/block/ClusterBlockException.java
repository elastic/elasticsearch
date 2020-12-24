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

package org.elasticsearch.cluster.block;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;

public class ClusterBlockException extends ElasticsearchException {
    private final Set<ClusterBlock> blocks;

    public ClusterBlockException(Set<ClusterBlock> globalLevelBlocks) {
        super(buildMessageForGlobalBlocks(globalLevelBlocks));
        this.blocks = globalLevelBlocks;
    }

    public ClusterBlockException(Map<String, Set<ClusterBlock>> indexLevelBlocks) {
        super(buildMessageForIndexBlocks(indexLevelBlocks));
        this.blocks = indexLevelBlocks.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
    }

    public ClusterBlockException(StreamInput in) throws IOException {
        super(in);
        this.blocks = unmodifiableSet(in.readSet(ClusterBlock::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (blocks != null) {
            out.writeCollection(blocks);
        } else {
            out.writeVInt(0);
        }
    }

    public boolean retryable() {
        for (ClusterBlock block : blocks) {
            if (!block.retryable()) {
                return false;
            }
        }
        return true;
    }

    public Set<ClusterBlock> blocks() {
        return blocks;
    }

    private static String buildMessageForGlobalBlocks(Set<ClusterBlock> globalLevelBlocks) {
        assert globalLevelBlocks.isEmpty() == false;
        Function<ClusterBlock, String> blockDescription = block -> block.status() + "/" + block.id() + "/" + block.description();
        StringBuilder sb = new StringBuilder();
        if (globalLevelBlocks.isEmpty() == false) {
            sb.append("blocked by: [");
            sb.append(globalLevelBlocks.stream().map(blockDescription).collect(Collectors.joining(", ")));
            sb.append("];");
        }
        return sb.toString();
    }

    private static String buildMessageForIndexBlocks(Map<String, Set<ClusterBlock>> indexLevelBlocks) {
        assert indexLevelBlocks.isEmpty() == false;
        Function<ClusterBlock, String> blockDescription = block -> block.status() + "/" + block.id() + "/" + block.description();
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Set<ClusterBlock>> entry : indexLevelBlocks.entrySet()) {
            sb.append("index [" + entry.getKey() + "] blocked by: [");
            sb.append(entry.getValue().stream().map(blockDescription).collect(Collectors.joining(", ")));
            sb.append("];");
        }
        return sb.toString();
    }

    @Override
    public RestStatus status() {
        RestStatus status = null;
        boolean onlyRetryableBlocks = true;
        for (ClusterBlock block : blocks) {
            boolean isRetryableBlock = block.status() == RestStatus.TOO_MANY_REQUESTS;
            if (isRetryableBlock == false) {
                if (status == null) {
                    status = block.status();
                } else if (status.getStatus() < block.status().getStatus()) {
                    status = block.status();
                }
            }
            onlyRetryableBlocks = onlyRetryableBlocks && isRetryableBlock;
        }
        // return retryable status if there are only retryable blocks
        if (onlyRetryableBlocks) {
            return RestStatus.TOO_MANY_REQUESTS;
        }
        // return status which has the maximum code of all status except the retryable blocks'
        return status;
    }
}
