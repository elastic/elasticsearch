/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Arrays;

/**
 * Like User, but includes the hashed password
 *
 * NOT to be used for password verification
 *
 * NOTE that this purposefully does not serialize the {@code passwordHash}
 * field, because this is not meant to be used for security other than
 * retrieving the UserAndPassword from the index before local authentication.
 */
class UserAndPassword {

    private final User user;
    private final char[] passwordHash;
    private final Hasher hasher;

    UserAndPassword(User user, char[] passwordHash) {
        this.user = user;
        this.passwordHash = passwordHash;
        this.hasher = Hasher.resolveFromHash(this.passwordHash);
    }

    public User user() {
        return this.user;
    }

    public char[] passwordHash() {
        return this.passwordHash;
    }

    boolean verifyPassword(SecureString data) {
        return hasher.verify(data, this.passwordHash);
    }

    @Override
    public boolean equals(Object o) {
        return false; // Don't use this for user comparison
    }

    @Override
    public int hashCode() {
        int result = this.user.hashCode();
        result = 31 * result + Arrays.hashCode(passwordHash());
        return result;
    }

    @Override
    public String toString() {
        return "{" + user + " with password hashed with :" + (passwordHash == null ? "<null>" : hasher.name()) + "}";
    }
}
