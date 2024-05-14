/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;

public class BulkShardOperationService implements BulkShardOperationProcessor {

    private final SetOnce<BulkShardOperationProcessor> bulkShardOperationProcessor = new SetOnce<>();

    public void setBulkShardOperationProcessor(BulkShardOperationProcessor processor) {
        bulkShardOperationProcessor.set(processor);
    }

    @Override
    public void apply(BulkShardRequest request, ClusterState clusterState, ActionListener<BulkShardRequest> listener) {
        BulkShardOperationProcessor processor = bulkShardOperationProcessor.get();
        if (processor == null) {
            listener.onResponse(request);
            return;
        }
        processor.apply(request, clusterState, listener);
    }
}
