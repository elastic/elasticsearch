/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for explaining evaluating search ranking results.
 */
public class RankEvalAction extends ActionType<RankEvalResponse> {

    public static final RankEvalAction INSTANCE = new RankEvalAction();
    public static final String NAME = "indices:data/read/rank_eval";

    private RankEvalAction() {
        super(NAME, RankEvalResponse::new);
    }
}
