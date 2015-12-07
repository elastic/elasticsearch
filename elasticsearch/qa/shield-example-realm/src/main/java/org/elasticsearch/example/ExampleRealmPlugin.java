/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example;

import org.elasticsearch.example.realm.CustomAuthenticationFailureHandler;
import org.elasticsearch.example.realm.CustomRealm;
import org.elasticsearch.example.realm.CustomRealmFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.authc.AuthenticationModule;

public class ExampleRealmPlugin extends Plugin {

    @Override
    public String name() {
        return "custom realm example";
    }

    @Override
    public String description() {
        return "a very basic implementation of a custom realm to validate it works";
    }

    public void onModule(AuthenticationModule authenticationModule) {
        authenticationModule.addCustomRealm(CustomRealm.TYPE, CustomRealmFactory.class);
        authenticationModule.setAuthenticationFailureHandler(CustomAuthenticationFailureHandler.class);
    }
}
