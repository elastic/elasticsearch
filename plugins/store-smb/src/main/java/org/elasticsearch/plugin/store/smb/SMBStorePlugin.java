/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.store.smb;

import org.elasticsearch.index.store.smb.SmbMmapFsDirectoryFactory;
import org.elasticsearch.index.store.smb.SmbNIOFSDirectoryFactory;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

public class SMBStorePlugin extends Plugin implements IndexStorePlugin {

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
