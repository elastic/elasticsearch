/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.waitforactiveshards;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActiveShardCount;

/**
 * {@link ActionFilter} which removes the {@code ?wait_for_active_shards} parameter from indexing requests since it is not meaningful in
 * stateless. We may decide to do something different with this parameter once we have a general facility for manipulating REST parameters
 * in ES-7547, and in that case this whole module can probably just be removed.
 */
public class SuppressWaitForActiveShardsActionFilter extends ActionFilter.Simple {
    @Override
    public int order() {
        return 0;
    }

    @Override
    protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
        if (action.equals(TransportBulkAction.NAME) && request instanceof BulkRequest bulkRequest) {
            // Single-item write requests become bulk requests in TransportSingleItemBulkWriteAction so this catches them all
            bulkRequest.waitForActiveShards(ActiveShardCount.NONE);
        }
        return true;
    }
}
