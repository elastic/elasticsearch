/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.util.OptionalLong;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_DIRECT_IO_DEFAULT_MERGE_BUFFER_SIZE_SETTING;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_DIRECT_IO_DEFAULT_MIN_BYTES_DIRECT_SETTING;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_USE_DIRECT_IO_FOR_MERGES_SETTING;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_USE_DIRECT_IO_FOR_SNAPSHOTS_SETTING;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_IO_CONTEXT;

public class ESDirectIODirectory extends DirectIODirectory {

    final boolean useDirectIOForMerges;
    final boolean useDirectIOForSnapshots;
    final long minBytesDirect;

    public ESDirectIODirectory(FSDirectory delegate, IndexSettings indexSettings) throws IOException {
        super(
            delegate,
            Math.toIntExact(indexSettings.getValue(INDEX_STORE_DIRECT_IO_DEFAULT_MERGE_BUFFER_SIZE_SETTING).getBytes()),
            indexSettings.getValue(INDEX_STORE_DIRECT_IO_DEFAULT_MIN_BYTES_DIRECT_SETTING).getBytes()
        );
        useDirectIOForMerges = indexSettings.getValue(INDEX_STORE_USE_DIRECT_IO_FOR_MERGES_SETTING);
        useDirectIOForSnapshots = indexSettings.getValue(INDEX_STORE_USE_DIRECT_IO_FOR_SNAPSHOTS_SETTING);
        minBytesDirect = indexSettings.getValue(INDEX_STORE_DIRECT_IO_DEFAULT_MIN_BYTES_DIRECT_SETTING).getBytes();
    }

    @Override
    protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
        if (useDirectIOForMerges && context.context == IOContext.Context.MERGE) {
            return super.useDirectIO(name, context, fileLength);
        }
        if (useDirectIOForSnapshots && context == SNAPSHOT_IO_CONTEXT) {
            return fileLength.orElse(minBytesDirect) >= minBytesDirect;
        }
        return false;
    }
}
