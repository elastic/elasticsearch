/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.user.privileges;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the privilege to "manage" certain applications. The "manage"
 * privilege is actually defined outside of Elasticsearch.
 */
public class ManageApplicationPrivilege extends GlobalOperationPrivilege {

    private static final String CATEGORY = "application";
    private static final String OPERATION = "manage";
    private static final String KEY = "applications";

    public ManageApplicationPrivilege(Collection<String> applications) {
        super(CATEGORY, OPERATION, Collections.singletonMap(KEY, new HashSet<String>(Objects.requireNonNull(applications))));
    }

    @SuppressWarnings("unchecked")
    public Set<String> getManagedApplications() {
        return (Set<String>) getRaw().get(KEY);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
