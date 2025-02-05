/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;

/**
 * Utilities for use during bootstrap. This is public so that tests may use these methods.
 */
public class BootstrapUtil {

    // no construction
    private BootstrapUtil() {}

    public static SecureSettings loadSecureSettings(Environment initialEnv, SecureString keystorePassword) throws BootstrapException {
        try {
            return KeyStoreWrapper.bootstrap(initialEnv.configDir(), () -> keystorePassword);
        } catch (Exception e) {
            throw new BootstrapException(e);
        }
    }
}
