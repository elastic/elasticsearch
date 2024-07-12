/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.stream.read.ReadContext;
import org.elasticsearch.common.blobstore.stream.write.WriteContext;

import java.io.IOException;

public interface AsyncMultiStreamBlobContainer extends BlobContainer {

    /**
     * Reads blob content from multiple streams, each from a specific part of the file, which is provided by the
     * StreamContextSupplier in the WriteContext passed to this method. An {@link IOException} is thrown if reading
     * any of the input streams fails, or writing to the target blob fails
     *
     * @param writeContext         A WriteContext object encapsulating all information needed to perform the upload
     * @param completionListener   Listener on which upload events should be published.
     * @throws IOException if any of the input streams could not be read, or the target blob could not be written to
     */
    void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException;

    /**
     * Creates an async callback of a {@link ReadContext} containing the multipart streams for a specified blob within the container.
     * @param blobName The name of the blob for which the {@link ReadContext} needs to be fetched.
     * @param listener  Async listener for {@link ReadContext} object which serves the input streams and other metadata for the blob
     */
    void readBlobAsync(String blobName, ActionListener<ReadContext> listener);

    /*
     * Wether underlying blobContainer can verify integrity of data after transfer. If true and if expected
     * checksum is provided in WriteContext, then the checksum of transferred data is compared with expected checksum
     * by underlying blobContainer. In this case, caller doesn't need to ensure integrity of data.
     */
    boolean remoteIntegrityCheckSupported();
}
