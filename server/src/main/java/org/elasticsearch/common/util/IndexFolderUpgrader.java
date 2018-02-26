/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Renames index folders from {index.name} to {index.uuid}
 */
public class IndexFolderUpgrader {
    private final NodeEnvironment nodeEnv;
    private final Settings settings;
    private final Logger logger = Loggers.getLogger(IndexFolderUpgrader.class);

    /**
     * Creates a new upgrader instance
     * @param settings node settings
     * @param nodeEnv the node env to operate on
     */
    IndexFolderUpgrader(Settings settings, NodeEnvironment nodeEnv) {
        this.settings = settings;
        this.nodeEnv = nodeEnv;
    }

    /**
     * Moves the index folder found in <code>source</code> to <code>target</code>
     */
    void upgrade(final Index index, final Path source, final Path target) throws IOException {
        boolean success = false;
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
            success = true;
        } catch (NoSuchFileException | FileNotFoundException exception) {
            // thrown when the source is non-existent because the folder was renamed
            // by another node (shared FS) after we checked if the target exists
            logger.error((Supplier<?>) () -> new ParameterizedMessage("multiple nodes trying to upgrade [{}] in parallel, retry " +
                "upgrading with single node", target), exception);
            throw exception;
        } finally {
            if (success) {
                logger.info("{} moved from [{}] to [{}]", index, source, target);
                logger.trace("{} syncing directory [{}]", index, target);
                IOUtils.fsync(target, true);
            }
        }
    }

    /**
     * Renames <code>indexFolderName</code> index folders found in node paths and custom path
     * iff {@link #needsUpgrade(Index, String)} is true.
     * Index folder in custom paths are renamed first followed by index folders in each node path.
     */
    void upgrade(final String indexFolderName) throws IOException {
        for (NodeEnvironment.NodePath nodePath : nodeEnv.nodePaths()) {
            final Path indexFolderPath = nodePath.indicesPath.resolve(indexFolderName);
            final IndexMetaData indexMetaData = IndexMetaData.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, indexFolderPath);
            if (indexMetaData != null) {
                final Index index = indexMetaData.getIndex();
                if (needsUpgrade(index, indexFolderName)) {
                    logger.info("{} upgrading [{}] to new naming convention", index, indexFolderPath);
                    final IndexSettings indexSettings = new IndexSettings(indexMetaData, settings);
                    if (indexSettings.hasCustomDataPath()) {
                        // we rename index folder in custom path before renaming them in any node path
                        // to have the index state under a not-yet-upgraded index folder, which we use to
                        // continue renaming after a incomplete upgrade.
                        final Path customLocationSource = nodeEnv.resolveBaseCustomLocation(indexSettings)
                            .resolve(indexFolderName);
                        final Path customLocationTarget = customLocationSource.resolveSibling(index.getUUID());
                        // we rename the folder in custom path only the first time we encounter a state
                        // in a node path, which needs upgrading, it is a no-op for subsequent node paths
                        if (Files.exists(customLocationSource) // might not exist if no data was written for this index
                            && Files.exists(customLocationTarget) == false) {
                            upgrade(index, customLocationSource, customLocationTarget);
                        } else {
                            logger.info("[{}] no upgrade needed - already upgraded", customLocationTarget);
                        }
                    }
                    upgrade(index, indexFolderPath, indexFolderPath.resolveSibling(index.getUUID()));
                } else {
                    logger.debug("[{}] no upgrade needed - already upgraded", indexFolderPath);
                }
            } else {
                logger.warn("[{}] no index state found - ignoring", indexFolderPath);
            }
        }
    }

    /**
     * Upgrades all indices found under <code>nodeEnv</code>. Already upgraded indices are ignored.
     */
    public static void upgradeIndicesIfNeeded(final Settings settings, final NodeEnvironment nodeEnv) throws IOException {
        final IndexFolderUpgrader upgrader = new IndexFolderUpgrader(settings, nodeEnv);
        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            upgrader.upgrade(indexFolderName);
        }
    }

    static boolean needsUpgrade(Index index, String indexFolderName) {
        return indexFolderName.equals(index.getUUID()) == false;
    }
}
