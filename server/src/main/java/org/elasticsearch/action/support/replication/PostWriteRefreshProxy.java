/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

public class PostWriteRefreshProxy {

    private static final Logger logger = LogManager.getLogger(PostWriteRefreshProxy.class);

    public void refreshShard(
        WriteRequest.RefreshPolicy policy,
        IndexShard indexShard,
        Translog.Location location,
        ActionListener<Boolean> listener
    ) {
        switch (policy) {
            case NONE -> listener.onResponse(false);
            case WAIT_UNTIL -> {
                if (location != null) {
                    indexShard.addRefreshListener(location, forcedRefresh -> {
                        if (forcedRefresh) {
                            logger.warn("block until refresh ran out of slots and forced a refresh: [{}]", policy);
//                logger.warn("block until refresh ran out of slots and forced a refresh: [{}]", request);
                        }
                        listener.onResponse(forcedRefresh);
                    });
                }
            }
            case IMMEDIATE -> indexShard.refresh("refresh_flag_index");
        }
    }
}
