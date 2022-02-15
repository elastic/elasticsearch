/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.action.ActionType;

public class ClusterSearchShardsAction extends ActionType<ClusterSearchShardsResponse> {

    public static final ClusterSearchShardsAction INSTANCE = new ClusterSearchShardsAction();
    public static final String NAME = "indices:admin/shards/search_shards";

    private ClusterSearchShardsAction() {
        super(NAME, ClusterSearchShardsResponse::new);
    }
}
