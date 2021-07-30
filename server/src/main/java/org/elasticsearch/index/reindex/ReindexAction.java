/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionType;

public class ReindexAction extends ActionType<BulkByScrollResponse> {
    public static final ReindexAction INSTANCE = new ReindexAction();
    public static final String NAME = "indices:data/write/reindex";

    private ReindexAction() {
        super(NAME, BulkByScrollResponse::new);
    }
}
