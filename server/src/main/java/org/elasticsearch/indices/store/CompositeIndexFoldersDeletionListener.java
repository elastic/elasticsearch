/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.store;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndexRemovalReason;
import org.elasticsearch.plugins.IndexStorePlugin;

import java.nio.file.Path;
import java.util.List;

public class CompositeIndexFoldersDeletionListener implements IndexStorePlugin.IndexFoldersDeletionListener {

    private final List<IndexStorePlugin.IndexFoldersDeletionListener> listeners;

    public CompositeIndexFoldersDeletionListener(List<IndexStorePlugin.IndexFoldersDeletionListener> listeners) {
        for (IndexStorePlugin.IndexFoldersDeletionListener listener : listeners) {
            if (listener == null) {
                throw new IllegalArgumentException("listeners must be non-null");
            }
        }
        this.listeners = List.copyOf(listeners);
    }

    @Override
    public void beforeIndexFoldersDeleted(Index index, IndexSettings indexSettings, Path[] indexPaths, IndexRemovalReason reason) {
        for (IndexStorePlugin.IndexFoldersDeletionListener listener : listeners) {
            try {
                listener.beforeIndexFoldersDeleted(index, indexSettings, indexPaths, reason);
            } catch (Exception e) {
                assert false : new AssertionError(e);
                throw e;
            }
        }
    }

    @Override
    public void beforeShardFoldersDeleted(ShardId shardId, IndexSettings indexSettings, Path[] shardPaths, IndexRemovalReason reason) {
        for (IndexStorePlugin.IndexFoldersDeletionListener listener : listeners) {
            try {
                listener.beforeShardFoldersDeleted(shardId, indexSettings, shardPaths, reason);
            } catch (Exception e) {
                assert false : new AssertionError(e);
                throw e;
            }
        }
    }
}
