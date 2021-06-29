/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.delete;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

/**
 * This action causes a dangling index to be considered as deleted by the cluster.
 */
public class DeleteDanglingIndexAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteDanglingIndexAction INSTANCE = new DeleteDanglingIndexAction();
    public static final String NAME = "cluster:admin/indices/dangling/delete";

    private DeleteDanglingIndexAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }
}
