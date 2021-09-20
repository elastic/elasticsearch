/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store.smb;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.store.FsDirectoryFactory;

import java.io.IOException;
import java.nio.file.Path;

public final class SmbNIOFSDirectoryFactory extends FsDirectoryFactory {

    @Override
    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
        return new SmbDirectoryWrapper(new NIOFSDirectory(location, lockFactory));
    }
}
