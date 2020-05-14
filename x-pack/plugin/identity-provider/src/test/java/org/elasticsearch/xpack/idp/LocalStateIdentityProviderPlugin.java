/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.security.Security;

import java.nio.file.Path;

public class LocalStateIdentityProviderPlugin extends LocalStateCompositeXPackPlugin {

    public LocalStateIdentityProviderPlugin(Settings settings, Path configPath) throws Exception {
        super(settings, configPath);
        plugins.add(new Security(settings, configPath));

        plugins.add(new IdentityProviderPlugin());
    }
}
