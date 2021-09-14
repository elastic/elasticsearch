/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.readonly;

import org.elasticsearch.action.ActionType;

public class AddIndexBlockAction extends ActionType<AddIndexBlockResponse> {

    public static final AddIndexBlockAction INSTANCE = new AddIndexBlockAction();
    public static final String NAME = "indices:admin/block/add";

    private AddIndexBlockAction() {
        super(NAME, AddIndexBlockResponse::new);
    }
}
