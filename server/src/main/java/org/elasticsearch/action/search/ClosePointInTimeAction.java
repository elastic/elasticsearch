/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionType;

public class ClosePointInTimeAction extends ActionType<ClosePointInTimeResponse> {

    public static final ClosePointInTimeAction INSTANCE = new ClosePointInTimeAction();
    public static final String NAME = "indices:data/read/close_point_in_time";

    private ClosePointInTimeAction() {
        super(NAME, ClosePointInTimeResponse::new);
    }
}
