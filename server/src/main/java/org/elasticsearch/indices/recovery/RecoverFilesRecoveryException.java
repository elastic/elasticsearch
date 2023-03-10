/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Objects;

public class RecoverFilesRecoveryException extends ElasticsearchException implements ElasticsearchWrapperException {

    private final int numberOfFiles;

    private final ByteSizeValue totalFilesSize;

    public RecoverFilesRecoveryException(ShardId shardId, int numberOfFiles, ByteSizeValue totalFilesSize, Throwable cause) {
        super("Failed to transfer [{}] files with total size of [{}]", cause, numberOfFiles, totalFilesSize);
        Objects.requireNonNull(totalFilesSize, "totalFilesSize must not be null");
        setShard(shardId);
        this.numberOfFiles = numberOfFiles;
        this.totalFilesSize = totalFilesSize;
    }

    public int numberOfFiles() {
        return numberOfFiles;
    }

    public ByteSizeValue totalFilesSize() {
        return totalFilesSize;
    }

    public RecoverFilesRecoveryException(StreamInput in) throws IOException {
        super(in);
        numberOfFiles = in.readInt();
        totalFilesSize = new ByteSizeValue(in);
    }

    @Override
    protected void writeTo(StreamOutput out, Writer<Throwable> nestedExceptionsWriter) throws IOException {
        super.writeTo(out, nestedExceptionsWriter);
        out.writeInt(numberOfFiles);
        totalFilesSize.writeTo(out);
    }
}
