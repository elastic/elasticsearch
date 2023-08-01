/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;

import java.io.IOException;

public abstract class ByteSizeDirectory extends FilterDirectory {

    protected ByteSizeDirectory(Directory in) {
        super(in);
    }

    /** Return the cumulative size of all files in this directory. */
    public abstract long estimateSizeInBytes() throws IOException;
}
