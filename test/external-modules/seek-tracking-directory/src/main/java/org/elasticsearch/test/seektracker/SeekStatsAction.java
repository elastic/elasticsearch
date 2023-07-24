/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.seektracker;

import org.elasticsearch.action.ActionType;

public class SeekStatsAction extends ActionType<SeekStatsResponse> {

    public static final SeekStatsAction INSTANCE = new SeekStatsAction();
    public static final String NAME = "cluster:monitor/seek_stats";

    public SeekStatsAction() {
        super(NAME, SeekStatsResponse::new);
    }
}
