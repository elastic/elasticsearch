/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.tvbackport;

import org.elasticsearch.action.ActionType;

public class TVBackportAction extends ActionType<TVBackportResponse> {
    public static final TVBackportAction INSTANCE = new TVBackportAction();
    public static final String NAME = "cluster:monitor/tvbackport";

    private TVBackportAction() {
        super(NAME);
    }
}
