/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;

public class LocalStateEncryptedRepositoryPlugin extends LocalStateCompositeXPackPlugin {

    final EncryptedRepositoryPlugin encryptedRepositoryPlugin;

    public LocalStateEncryptedRepositoryPlugin(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        LocalStateEncryptedRepositoryPlugin thisVar = this;

        encryptedRepositoryPlugin = new EncryptedRepositoryPlugin(settings) {
            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }
        };
        plugins.add(encryptedRepositoryPlugin);
    }

}
