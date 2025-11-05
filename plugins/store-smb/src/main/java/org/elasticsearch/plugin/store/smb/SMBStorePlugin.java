/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.store.smb;

import org.elasticsearch.index.store.smb.SmbMmapFsDirectoryFactory;
import org.elasticsearch.index.store.smb.SmbNIOFSDirectoryFactory;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

/**
 * Elasticsearch plugin that provides SMB (Server Message Block) store implementations.
 * This plugin enables Elasticsearch to use SMB/CIFS network file systems for index storage
 * by wrapping Lucene directory implementations with SMB-specific optimizations to avoid
 * problematic file operations on Windows network shares.
 */
public class SMBStorePlugin extends Plugin implements IndexStorePlugin {

    /**
     * Provides directory factories for SMB-based index storage.
     * Offers multiple directory implementations optimized for SMB/CIFS shares:
     * <ul>
     * <li>smb_mmap_fs: Memory-mapped file access (recommended for 64-bit systems)</li>
     * <li>smb_simple_fs: Simple file system access (legacy alias for smb_nio_fs)</li>
     * <li>smb_nio_fs: NIO-based file system access</li>
     * </ul>
     *
     * @return a map of store type names to their corresponding directory factories
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * PUT /my-index
     * {
     *   "settings": {
     *     "index.store.type": "smb_mmap_fs"
     *   }
     * }
     * }</pre>
     */
    @Override
    public Map<String, DirectoryFactory> getDirectoryFactories() {
        return Map.of(
            "smb_mmap_fs",
            new SmbMmapFsDirectoryFactory(),
            "smb_simple_fs",
            new SmbNIOFSDirectoryFactory(),
            "smb_nio_fs",
            new SmbNIOFSDirectoryFactory()
        );
    }

}
