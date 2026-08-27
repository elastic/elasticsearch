/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.example.implicit;

import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;

import java.util.List;

/**
 * SPI-loaded {@link SecurityExtension} that registers {@link HelicarrierImplicitPrivilegesProvider}.
 * The internal-cluster integration tests exercise this same provider via
 * {@code LocalStateWithImplicitPrivileges}; this QA module wires it into a real plugin so the
 * full HTTP get-roles surface (including the {@code include_implicit} query parameter and the
 * {@code implicitly_granted} response field) can be asserted against a real cluster.
 */
public class ImplicitPrivilegesTestExtension implements SecurityExtension {

    @Override
    public String extensionName() {
        return "implicit-privileges-extension-test";
    }

    @Override
    public List<ImplicitPrivilegesProvider> getImplicitPrivilegesProviders(SecurityComponents components) {
        return List.of(new HelicarrierImplicitPrivilegesProvider());
    }
}
