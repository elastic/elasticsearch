/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.security.authz.microsoft;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authc.Realm;

import java.util.List;
import java.util.Map;

public class MicrosoftGraphAuthzPlugin extends Plugin implements SecurityExtension {
    @Override
    public Map<String, Realm.Factory> getRealms(SecurityComponents components) {
        return Map.of(MicrosoftGraphAuthzRealmSettings.REALM_TYPE, MicrosoftGraphAuthzRealm::new);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return MicrosoftGraphAuthzRealmSettings.getSettings();
    }
}
