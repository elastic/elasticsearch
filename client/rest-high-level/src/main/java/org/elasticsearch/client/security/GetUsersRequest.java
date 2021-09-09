/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Request object to retrieve users from the native realm
 */
public class GetUsersRequest implements Validatable {
    private final Set<String> usernames;

    public GetUsersRequest(final String... usernames) {
        if (usernames != null) {
            this.usernames = Collections.unmodifiableSet(Sets.newHashSet(usernames));
        } else {
            this.usernames = Collections.emptySet();
        }
    }

    public Set<String> getUsernames() {
        return usernames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof GetUsersRequest) == false) return false;
        GetUsersRequest that = (GetUsersRequest) o;
        return Objects.equals(usernames, that.usernames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(usernames);
    }
}
