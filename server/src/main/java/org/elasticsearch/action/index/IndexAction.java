/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.index;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;

public class IndexAction extends ActionType<DocWriteResponse> {

    public static final IndexAction INSTANCE = new IndexAction();
    public static final String NAME = "indices:data/write/index";

    private IndexAction() {
        super(NAME, in -> {
            assert false : "Might not be an IndexResponse!";
            return new IndexResponse(in);
        });
    }
}
