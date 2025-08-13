/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.xpack.core.security.action.GrantRequest;

public class ActivateProfileRequest extends GrantRequest {

    public ActivateProfileRequest() {
        super();
    }

    @Override
    public ActionRequestValidationException validate() {
        return super.validate();
    }
}
