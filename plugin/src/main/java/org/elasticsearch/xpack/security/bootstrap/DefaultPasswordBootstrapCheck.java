/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.bootstrap;

import java.util.Locale;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;

public class DefaultPasswordBootstrapCheck implements BootstrapCheck {

    private Settings settings;

    public DefaultPasswordBootstrapCheck(Settings settings) {
        this.settings = settings;
    }

    /**
     * This check fails if the "accept default password" is set to <code>true</code>.
     */
    @Override
    public boolean check() {
        return ReservedRealm.ACCEPT_DEFAULT_PASSWORD_SETTING.get(settings) == true;
    }

    @Override
    public String errorMessage() {
        return String.format(Locale.ROOT, "The configuration setting '%s' is %s - it should be set to false",
                ReservedRealm.ACCEPT_DEFAULT_PASSWORD_SETTING.getKey(),
                ReservedRealm.ACCEPT_DEFAULT_PASSWORD_SETTING.exists(settings) ? "set to true" : "not set"
        );
    }
}
