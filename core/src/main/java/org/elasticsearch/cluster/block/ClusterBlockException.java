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
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public class ClusterBlockException extends ElasticsearchException {
    private final Set<ClusterBlock> blocks;

    public ClusterBlockException(Set<ClusterBlock> blocks) {
        super(buildMessage(blocks));
        this.blocks = blocks;
    }

    public ClusterBlockException(StreamInput in) throws IOException {
        super(in);
        int totalBlocks = in.readVInt();
        Set<ClusterBlock> blocks = new HashSet<>(totalBlocks);
        for (int i = 0; i < totalBlocks;i++) {
            blocks.add(ClusterBlock.readClusterBlock(in));
        }
        this.blocks = unmodifiableSet(blocks);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (blocks != null) {
            out.writeVInt(blocks.size());
            for (ClusterBlock block : blocks) {
                block.writeTo(out);
            }
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

    private static String buildMessage(Set<ClusterBlock> blocks) {
        StringBuilder sb = new StringBuilder("blocked by: ");
        for (ClusterBlock block : blocks) {
            sb.append("[").append(block.status()).append("/").append(block.id()).append("/").append(block.description()).append("];");
        }
        return sb.toString();
    }

    @Override
    public RestStatus status() {
        RestStatus status = null;
        for (ClusterBlock block : blocks) {
            if (status == null) {
                status = block.status();
            } else if (status.getStatus() < block.status().getStatus()) {
                status = block.status();
            }
        }
        return status;
    }
}
