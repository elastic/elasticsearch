/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.ActionType;

public class PrevalidateNodeRemovalAction extends ActionType<PrevalidateNodeRemovalResponse> {

    public static final PrevalidateNodeRemovalAction INSTANCE = new PrevalidateNodeRemovalAction();
    public static final String NAME = "cluster:admin/shutdown/prevalidate_removal";

    private PrevalidateNodeRemovalAction() {
        super(NAME, PrevalidateNodeRemovalResponse::new);
    }

}
