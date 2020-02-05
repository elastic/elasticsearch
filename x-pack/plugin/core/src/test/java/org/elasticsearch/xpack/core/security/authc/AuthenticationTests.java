/*
 *
 *  * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 *  * or more contributor license agreements. Licensed under the Elastic License;
 *  * you may not use this file except in compliance with the Elastic License.
 *
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.user.User;

public class AuthenticationTests extends ESTestCase {

    public void testWillGetLookedUpByWhenItExists() {
        final Authentication.RealmRef authenticatedBy = new Authentication.RealmRef("auth_by", "auth_by_type", "node");
        final Authentication.RealmRef lookedUpBy = new Authentication.RealmRef("lookup_by", "lookup_by_type", "node");
        final Authentication authentication = new Authentication(
            new User("user"), authenticatedBy, lookedUpBy);

        assertEquals(lookedUpBy, authentication.getSourceRealm());
    }

    public void testWillGetAuthenticateByWhenLookupIsNull() {
        final Authentication.RealmRef authenticatedBy = new Authentication.RealmRef("auth_by", "auth_by_type", "node");
        final Authentication authentication = new Authentication(
            new User("user"), authenticatedBy, null);

        assertEquals(authenticatedBy, authentication.getSourceRealm());
    }

}
