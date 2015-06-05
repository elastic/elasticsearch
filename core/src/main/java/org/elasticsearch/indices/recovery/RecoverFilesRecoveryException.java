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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.IndexShardException;
import org.elasticsearch.index.shard.ShardId;

/**
 *
 */
public class RecoverFilesRecoveryException extends IndexShardException implements ElasticsearchWrapperException {

    private final int numberOfFiles;

    private final ByteSizeValue totalFilesSize;

    public RecoverFilesRecoveryException(ShardId shardId, int numberOfFiles, ByteSizeValue totalFilesSize, Throwable cause) {
        super(shardId, "Failed to transfer [" + numberOfFiles + "] files with total size of [" + totalFilesSize + "]", cause);
        this.numberOfFiles = numberOfFiles;
        this.totalFilesSize = totalFilesSize;
    }

    public int numberOfFiles() {
        return numberOfFiles;
    }

    public ByteSizeValue totalFilesSize() {
        return totalFilesSize;
    }
}
