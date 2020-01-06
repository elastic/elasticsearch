/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action.compat;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

public class DeleteTransformActionDeprecated extends ActionType<AcknowledgedResponse> {

    public static final DeleteTransformActionDeprecated INSTANCE = new DeleteTransformActionDeprecated();
    public static final String NAME = "cluster:admin/data_frame/delete";

    private DeleteTransformActionDeprecated() {
        super(NAME, AcknowledgedResponse::new);
    }
}
