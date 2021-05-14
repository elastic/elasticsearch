/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

public class DeleteIndexAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteIndexAction INSTANCE = new DeleteIndexAction();
    public static final String NAME = "indices:admin/delete";

    private DeleteIndexAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

}
