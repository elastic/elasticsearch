/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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

}
