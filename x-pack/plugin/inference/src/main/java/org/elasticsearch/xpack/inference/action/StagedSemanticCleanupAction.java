/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for the {@code POST /{index}/_semantic_cleanup} endpoint, which
 * clears expired staged semantic_text data across all shards of the target indices.
 */
public class StagedSemanticCleanupAction extends ActionType<StagedSemanticCleanupResponse> {

    public static final StagedSemanticCleanupAction INSTANCE = new StagedSemanticCleanupAction();
    public static final String NAME = "indices:admin/semantic_cleanup";

    private StagedSemanticCleanupAction() {
        super(NAME);
    }
}
