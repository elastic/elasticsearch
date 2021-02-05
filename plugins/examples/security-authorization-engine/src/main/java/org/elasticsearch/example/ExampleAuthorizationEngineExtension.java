/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;

/**
 * Security extension class that registers the custom authorization engine to be used
 */
public class ExampleAuthorizationEngineExtension implements SecurityExtension {

    @Override
    public AuthorizationEngine getAuthorizationEngine(Settings settings) {
        return new CustomAuthorizationEngine();
    }
}
