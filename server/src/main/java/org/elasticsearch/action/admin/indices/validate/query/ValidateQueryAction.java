/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.ActionType;

public class ValidateQueryAction extends ActionType<ValidateQueryResponse> {

    public static final ValidateQueryAction INSTANCE = new ValidateQueryAction();
    public static final String NAME = "indices:admin/validate/query";

    private ValidateQueryAction() {
        super(NAME, ValidateQueryResponse::new);
    }
}
