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

import java.util.Collection;

public final class NoFsyncDirectory extends FilterDirectory {
    public NoFsyncDirectory(Directory in) {
        super(in);
    }

    @Override
    public void sync(Collection<String> names) {
        // noop
    }

    @Override
    public void syncMetaData() {
        // noop
    }
}
