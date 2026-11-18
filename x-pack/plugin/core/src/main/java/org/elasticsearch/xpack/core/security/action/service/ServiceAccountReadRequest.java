/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.core.Nullable;

/**
 * A request that reads service accounts and that authorization may narrow to the accounts a caller's
 * {@link org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageServiceAccountsPrivilege}
 * grants cover. The restriction is set only by authorization, never by the caller, and is not serialized: every node
 * that receives a request authorizes it afresh.
 */
public interface ServiceAccountReadRequest {

    /**
     * The accounts this request is restricted to, or {@code null} when it reads every account it selects.
     */
    @Nullable
    AuthorizedServiceAccounts getAuthorizedAccounts();

    void restrictToAuthorizedAccounts(AuthorizedServiceAccounts authorizedAccounts);
}
