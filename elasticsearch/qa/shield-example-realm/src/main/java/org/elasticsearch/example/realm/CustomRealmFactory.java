/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.RealmConfig;

public class CustomRealmFactory extends Realm.Factory<CustomRealm> {

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
