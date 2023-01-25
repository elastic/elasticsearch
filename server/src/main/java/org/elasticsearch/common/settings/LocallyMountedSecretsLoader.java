/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;

import java.util.Optional;

public class LocallyMountedSecretsLoader implements SecureSettingsLoader {
    @Override
    public LoadedSecrets load(Environment environment, Terminal terminal)
        throws Exception {
        return new LoadedSecrets(new LocallyMountedSecrets(environment), Optional.empty());
    }

    @Override
    public SecureSettings bootstrap(Environment environment, SecureString password) throws Exception {
        return new LocallyMountedSecrets(environment);
    }

    @Override
    public boolean supportsAutoConfigure() {
        return false;
    }
}
