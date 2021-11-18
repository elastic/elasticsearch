/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.support;

import org.elasticsearch.common.blobstore.BlobMetadata;

public class PlainBlobMetadata implements BlobMetadata {

    private final String name;

    private final long length;

    public PlainBlobMetadata(String name, long length) {
        this.name = name;
        this.length = length;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public long length() {
        return this.length;
    }

    @Override
    public String toString() {
        return "name [" + name + "], length [" + length + "]";
    }
}
