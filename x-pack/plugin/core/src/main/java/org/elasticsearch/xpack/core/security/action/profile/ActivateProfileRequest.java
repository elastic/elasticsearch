/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.xpack.core.security.action.GrantRequest;

import java.util.Set;

import static org.elasticsearch.xpack.core.security.action.Grant.ACCESS_TOKEN_GRANT_TYPE;
import static org.elasticsearch.xpack.core.security.action.Grant.PASSWORD_GRANT_TYPE;

public class ActivateProfileRequest extends GrantRequest {

    public ActivateProfileRequest() {
        super(Set.of(PASSWORD_GRANT_TYPE, ACCESS_TOKEN_GRANT_TYPE));
    }
}
