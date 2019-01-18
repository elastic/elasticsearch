/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
