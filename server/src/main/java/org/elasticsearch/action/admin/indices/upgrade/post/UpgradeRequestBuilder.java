/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.upgrade.post;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A request to upgrade one or more indices. In order to optimize on all the indices, pass an empty array or
 * {@code null} for the indices.
 */
public class UpgradeRequestBuilder extends BroadcastOperationRequestBuilder<UpgradeRequest, UpgradeResponse, UpgradeRequestBuilder> {

    public UpgradeRequestBuilder(ElasticsearchClient client, UpgradeAction action) {
        super(client, action, new UpgradeRequest());
    }

    /**
     *  Should the upgrade only the ancient (older major version of Lucene) segments?
     */
    public UpgradeRequestBuilder setUpgradeOnlyAncientSegments(boolean upgradeOnlyAncientSegments) {
        request.upgradeOnlyAncientSegments(upgradeOnlyAncientSegments);
        return this;
    }
}
