/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionType;

public class GetProfilesAction extends ActionType<GetProfilesResponse> {

    public static final String NAME = "cluster:admin/xpack/security/profile/get";
    public static final GetProfilesAction INSTANCE = new GetProfilesAction();

    public GetProfilesAction() {
        super(NAME, GetProfilesResponse::new);
    }
}
