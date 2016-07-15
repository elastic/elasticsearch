/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.RealmConfig;

public class CustomRealmFactory extends Realm.Factory<CustomRealm> {

    @Inject
    public CustomRealmFactory() {
        super(CustomRealm.TYPE, false);
    }

    @Override
    public CustomRealm create(RealmConfig config) {
        return new CustomRealm(config);
    }

    @Override
    public CustomRealm createDefault(String name) {
        return null;
    }
}
