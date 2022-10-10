/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.compute.transport;

import org.elasticsearch.action.ActionType;

public class ComputeAction2 extends ActionType<ComputeResponse> {

    public static final ComputeAction2 INSTANCE = new ComputeAction2();
    public static final String NAME = "indices:data/read/compute2";

    private ComputeAction2() {
        super(NAME, ComputeResponse::new);
    }

}
