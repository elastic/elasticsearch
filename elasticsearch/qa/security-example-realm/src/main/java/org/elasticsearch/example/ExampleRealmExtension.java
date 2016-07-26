/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example;

import org.elasticsearch.example.realm.CustomAuthenticationFailureHandler;
import org.elasticsearch.example.realm.CustomRealm;
import org.elasticsearch.xpack.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.extensions.XPackExtension;
import org.elasticsearch.xpack.security.authc.Realm;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class ExampleRealmExtension extends XPackExtension {

    static {
        // check that the extension's policy works.
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            System.getSecurityManager().checkPrintJobAccess();
            return null;
        });
    }

    @Override
    public String name() {
        return "custom realm example";
    }

    @Override
    public String description() {
        return "a very basic implementation of a custom realm to validate it works";
    }

    @Override
    public Map<String, Realm.Factory> getRealms() {
        return Collections.singletonMap(CustomRealm.TYPE, CustomRealm::new);
    }

    @Override
    public AuthenticationFailureHandler getAuthenticationFailureHandler() {
        return new CustomAuthenticationFailureHandler();
    }

    @Override
    public Collection<String> getRestHeaders() {
        return Arrays.asList(CustomRealm.USER_HEADER, CustomRealm.PW_HEADER);
    }
}
