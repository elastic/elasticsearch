/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;

public class ClearVotingConfigExclusionsAction extends ActionType<ActionResponse.Empty> {
    public static final ClearVotingConfigExclusionsAction INSTANCE = new ClearVotingConfigExclusionsAction();
    public static final String NAME = "cluster:admin/voting_config/clear_exclusions";

    private ClearVotingConfigExclusionsAction() {
        super(NAME, in -> ActionResponse.Empty.INSTANCE);
    }
}
