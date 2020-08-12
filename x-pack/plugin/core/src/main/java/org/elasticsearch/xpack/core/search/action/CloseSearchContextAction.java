/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionType;

public class CloseSearchContextAction extends ActionType<CloseSearchContextResponse> {

    public static final CloseSearchContextAction INSTANCE = new CloseSearchContextAction();
    public static final String NAME = "indices:data/read/close_search_context";

    private CloseSearchContextAction() {
        super(NAME, CloseSearchContextResponse::new);
    }
}
