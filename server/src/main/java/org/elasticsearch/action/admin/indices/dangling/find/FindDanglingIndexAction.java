/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.dangling.find;

import org.elasticsearch.action.ActionType;

/**
 * Represents a request to find a particular dangling index by UUID.
 */
public class FindDanglingIndexAction extends ActionType<FindDanglingIndexResponse> {

    public static final FindDanglingIndexAction INSTANCE = new FindDanglingIndexAction();
    public static final String NAME = "cluster:admin/indices/dangling/find";

    private FindDanglingIndexAction() {
        super(NAME, FindDanglingIndexResponse::new);
    }
}
