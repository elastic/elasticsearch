/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.list;

import org.elasticsearch.action.ActionType;

/**
 * Represents a request to list all dangling indices known to the cluster.
 */
public class ListDanglingIndicesAction extends ActionType<ListDanglingIndicesResponse> {

    public static final ListDanglingIndicesAction INSTANCE = new ListDanglingIndicesAction();
    public static final String NAME = "cluster:admin/indices/dangling/list";

    private ListDanglingIndicesAction() {
        super(NAME, ListDanglingIndicesResponse::new);
    }
}
