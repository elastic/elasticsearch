/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionType;

/**
 * Tells which queries of a version of the golden dataset have a ground truth that is out of date. It is an admin action:
 * it runs searches over the data of the users, with the privileges of whoever calls it.
 */
public final class QuerySamplingGoldenCheckAction extends ActionType<QuerySamplingGoldenCheckResponse> {

    public static final QuerySamplingGoldenCheckAction INSTANCE = new QuerySamplingGoldenCheckAction();

    public static final String NAME = "cluster:admin/xpack/query_sampling/golden/check";

    private QuerySamplingGoldenCheckAction() {
        super(NAME);
    }
}
