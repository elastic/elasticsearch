/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.oldcodecs;

import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.index.store.FsDirectoryFactory.INDEX_LOCK_FACTOR_SETTING;

public class Lucene5Plugin extends Plugin implements RepositoryPlugin, IndexStorePlugin, EnginePlugin {

    public static final String LUCENE5_STORE_TYPE = "lucene5";

    public static boolean isLucene5Store(Settings indexSettings) {
        return LUCENE5_STORE_TYPE.equals(INDEX_STORE_TYPE_SETTING.get(indexSettings));
    }

    @SuppressWarnings("unused")
    public Lucene5Plugin() {
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry,
                                                           ClusterService clusterService, BigArrays bigArrays,
                                                           RecoverySettings recoverySettings) {
        return Collections.singletonMap("lucene5", Lucene5SnapshotRepository.newRepositoryFactory(
            clusterService.getClusterApplierService().threadPool()
        ));
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        return Optional.empty();
//        return config -> new ReadOnlyEngine(config, null, new TranslogStats(0, 0, 0, 0, 0), true,
//            readerWrapper(config), true, false);
    }

    @Override
    public Map<String, IndexStorePlugin.DirectoryFactory> getDirectoryFactories() {
        return Map.of(LUCENE5_STORE_TYPE, (indexSettings, shardPath) -> {
            //return new InMemoryNoOpCommitDirectory(new FsDirectoryFactory().newDirectory(indexSettings, shardPath));
            final Path location = shardPath.resolveIndex();
            final LockFactory lockFactory = indexSettings.getValue(INDEX_LOCK_FACTOR_SETTING);
            Files.createDirectories(location);
            return new NIOFSDirectory(location, lockFactory);
        });
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (isLucene5Store(indexModule.getSettings())) {
            indexModule.addIndexEventListener(new Lucene5IndexEventListener());
        }
    }
}
