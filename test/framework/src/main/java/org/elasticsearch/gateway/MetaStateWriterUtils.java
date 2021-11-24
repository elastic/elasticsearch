/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

/**
 * Maintains the method of writing cluster states to disk for versions prior to {@link Version#V_7_6_0}, preserved to test the classes that
 * read this state during an upgrade from these older versions.
 */
public class MetaStateWriterUtils {
    private static final Logger logger = LogManager.getLogger(MetaStateWriterUtils.class);

    private MetaStateWriterUtils() {
        throw new AssertionError("static methods only");
    }

    /**
     * Writes manifest file (represented by {@link Manifest}) to disk and performs cleanup of old manifest state file if
     * the write succeeds or newly created manifest state if the write fails.
     *
     * @throws WriteStateException if exception when writing state occurs. See also {@link WriteStateException#isDirty()}
     */
    public static void writeManifestAndCleanup(NodeEnvironment nodeEnv, String reason, Manifest manifest) throws WriteStateException {
        logger.trace("[_meta] writing state, reason [{}]", reason);
        try {
            long generation = Manifest.FORMAT.writeAndCleanup(manifest, nodeEnv.nodeDataPaths());
            logger.trace("[_meta] state written (generation: {})", generation);
        } catch (WriteStateException ex) {
            throw new WriteStateException(ex.isDirty(), "[_meta]: failed to write meta state", ex);
        }
    }

    /**
     * Writes the index state.
     * <p>
     * This method is public for testing purposes.
     *
     * @throws WriteStateException if exception when writing state occurs. {@link WriteStateException#isDirty()} will always return
     *                             false, because new index state file is not yet referenced by manifest file.
     */
    public static long writeIndex(NodeEnvironment nodeEnv, String reason, IndexMetadata indexMetadata) throws WriteStateException {
        final Index index = indexMetadata.getIndex();
        logger.trace("[{}] writing state, reason [{}]", index, reason);
        try {
            long generation = IndexMetadata.FORMAT.write(indexMetadata, nodeEnv.indexPaths(indexMetadata.getIndex()));
            logger.trace("[{}] state written", index);
            return generation;
        } catch (WriteStateException ex) {
            throw new WriteStateException(false, "[" + index + "]: failed to write index state", ex);
        }
    }

    /**
     * Writes the global state, *without* the indices states.
     *
     * @throws WriteStateException if exception when writing state occurs. {@link WriteStateException#isDirty()} will always return
     *                             false, because new global state file is not yet referenced by manifest file.
     */
    static long writeGlobalState(NodeEnvironment nodeEnv, String reason, Metadata metadata) throws WriteStateException {
        logger.trace("[_global] writing state, reason [{}]", reason);
        try {
            long generation = Metadata.FORMAT.write(metadata, nodeEnv.nodeDataPaths());
            logger.trace("[_global] state written");
            return generation;
        } catch (WriteStateException ex) {
            throw new WriteStateException(false, "[_global]: failed to write global state", ex);
        }
    }

}
