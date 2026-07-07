/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;

import java.util.List;

/**
 * Provides implicit security privileges for Kibana "as-data" features.
 *
 * <p>This plugin extends the security plugin to inject index-level privileges
 * derived from Kibana application privileges, enabling users to search Kibana
 * resources (such as alerts) directly in Elasticsearch while respecting
 * Kibana's space-based access control.
 */
public class KibanaSecurityPlugin extends Plugin implements SecurityExtension {

    private static final List<ImplicitPrivilegesProvider> PROVIDERS = List.of(new KibanaAlertsImplicitPrivilegesProvider());

    @Override
    public List<ImplicitPrivilegesProvider> getImplicitPrivilegesProviders(SecurityComponents components) {
        return PROVIDERS;
    }
}
