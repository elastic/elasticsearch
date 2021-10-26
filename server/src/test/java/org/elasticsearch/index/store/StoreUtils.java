/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;

import java.nio.file.Path;

public final class StoreUtils {

    /**
     * Returns {@code true} iff the given location contains an index an the index
     * can be successfully opened. This includes reading the segment infos and possible
     * corruption markers.
     */
    public static boolean canOpenIndex(Logger logger, Path indexLocation,
                                            ShardId shardId, NodeEnvironment.ShardLocker shardLocker) {
        try {
            Store.tryOpenIndex(indexLocation, shardId, shardLocker, logger);
        } catch (Exception ex) {
            logger.trace(() -> new ParameterizedMessage("Can't open index for path [{}]", indexLocation), ex);
            return false;
        }
        return true;
    }
}
