/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;

public class FlushAction extends ActionType<BroadcastResponse> {

    public static final FlushAction INSTANCE = new FlushAction();
    public static final String NAME = "indices:admin/flush";

    private FlushAction() {
        super(NAME);
    }
}
