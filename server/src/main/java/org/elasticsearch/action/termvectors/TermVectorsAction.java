/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.action.ActionType;

public class TermVectorsAction extends ActionType<TermVectorsResponse> {

    public static final TermVectorsAction INSTANCE = new TermVectorsAction();
    public static final String NAME = "indices:data/read/tv";

    private TermVectorsAction() {
        super(NAME);
    }

}
