/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.translog.transfer;

public class FileTransferException extends RuntimeException {

    private final FileSnapshot.TransferFileSnapshot fileSnapshot;

    public FileTransferException(FileSnapshot.TransferFileSnapshot fileSnapshot, Throwable cause) {
        super(cause);
        this.fileSnapshot = fileSnapshot;
    }

    public FileSnapshot.TransferFileSnapshot getFileSnapshot() {
        return fileSnapshot;
    }
}
