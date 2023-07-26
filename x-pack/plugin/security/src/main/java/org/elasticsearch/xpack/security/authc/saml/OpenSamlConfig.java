/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.opensaml.core.config.ConfigurationPropertiesSource;

import java.util.Properties;

public class OpenSamlConfig implements ConfigurationPropertiesSource {
    @Override
    public Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("opensaml.config.ecdh.defaultKDF","PBKDF2");
        return properties;
    }
}
