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

import java.nio.file.Files;

public class LocallyMountedSecretsLoader implements SecureSettingsLoader {
    @Override
    public SecureSettings load(Environment environment, Terminal terminal, AutoConfigureFunction<SecureString, Environment> autoConfigure)
        throws Exception {
        SecureSettings secrets = new LocallyMountedSecrets(environment);
        autoConfigure.apply(new SecureString(new char[0]));
        return secrets;
    }

    @Override
    public SecureSettings load(Environment environment, char[] password) throws Exception {
        return new LocallyMountedSecrets(environment);
    }

    @Override
    public String validate(Environment environment) {
        if (Files.exists(environment.configFile()) == false) {
            throw new IllegalStateException("config directory must exist for locally mounted secrets");
        }
        return null;
    }
}
