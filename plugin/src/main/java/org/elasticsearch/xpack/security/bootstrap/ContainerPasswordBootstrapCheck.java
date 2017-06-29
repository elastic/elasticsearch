/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.bootstrap;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.xpack.security.authc.ContainerSettings;

/**
 * A bootstrap check validating container environment variables. The bootstrap password option
 * cannot be present if the container environment variable is not set to true.
 */
public final class ContainerPasswordBootstrapCheck implements BootstrapCheck {

    private final ContainerSettings containerSettings;

    public ContainerPasswordBootstrapCheck() {
        this(ContainerSettings.parseAndCreate());
    }

    public ContainerPasswordBootstrapCheck(ContainerSettings containerSettings) {
        this.containerSettings = containerSettings;
    }

    @Override
    public boolean check() {
        if (containerSettings.getPasswordHash() != null && containerSettings.inContainer() == false) {
            return true;
        }
        return false;
    }

    @Override
    public String errorMessage() {
        return "Cannot use bootstrap password env variable [" + ContainerSettings.BOOTSTRAP_PASSWORD_ENV_VAR + "] if " +
                "Elasticsearch is not being deployed in a container.";
    }
}
