/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.common.Strings;

import java.io.IOException;

/// thrown when a blob read is modified (i.e., the read stops and when it resumes is sees a different version)
public class BlobVersionMismatchException extends IOException {

    public BlobVersionMismatchException(String blobName, String version, Throwable cause) {
        super(
            Strings.format(
                "blob [%s] version [%s] is no longer available: contents were modified or the blob was deleted while being read",
                blobName,
                version
            ),
            cause
        );
    }
}
