/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.core.RefCounted;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.io.InputStream;

/// A blob to upload to the object store: what to write and how to read its bytes. The caller keeps the source of the bytes readable
/// until the listener of the upload is completed, and the upload holds a reference of its own while it writes the blob.
public interface UploadableBlob extends RefCounted {

    ShardId shardId();

    Blob blob();

    long length();

    /// Opens a stream of the whole blob, from its start. It is called at most once per attempt, so again if the upload is retried. The
    /// stream must support [InputStream#mark(int)] and [InputStream#reset()] without buffering the bytes: the repositories reset it,
    /// possibly to its start, when they retry a request. It can be closed more than once.
    InputStream openStream() throws IOException;

    /// @return `true` if [#openStream(long, long)] is supported, which allows to upload the parts of the blob concurrently
    boolean supportsPartialStreams();

    /// Opens a stream of a part of the blob, only called if [#supportsPartialStreams()] is `true`. The parts are opened from several
    /// threads, and a part can be opened several times when the repository retries it, possibly after the write failed, the reference
    /// of the upload was released and the listener of the attempt was completed: a call that finds the source gone must throw. Each
    /// stream must keep its part of the source readable until it is closed, which can happen more than once, and must support
    /// [InputStream#mark(int)] and [InputStream#reset()] without buffering the bytes.
    ///
    /// @param offset the offset of the part in the blob
    /// @param length the length of the part
    InputStream openStream(long offset, long length) throws IOException;
}
