/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.shield.authz.store.FileRolesStore;
import org.elasticsearch.shield.authz.store.RolesStore;

/**
 *
 */
public class AuthorizationModule extends AbstractModule {

    @Override
    protected void configure() {

        bind(RolesStore.class).to(FileRolesStore.class);
        bind(AuthorizationService.class).to(InternalAuthorizationService.class);
    }
}
