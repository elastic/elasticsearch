/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.security.authc.support.Hasher;

/**
 * Parses and stores environment settings relevant to running Elasticsearch in a container.
 */
public final class ContainerSettings {

    public static final String BOOTSTRAP_PASSWORD_ENV_VAR = "BOOTSTRAP_PWD";
    public static final String CONTAINER_ENV_VAR = "ELASTIC_CONTAINER";

    private final boolean inContainer;
    private final char[] passwordHash;

    public ContainerSettings(boolean inContainer, char[] passwordHash) {
        this.inContainer = inContainer;
        this.passwordHash = passwordHash;
    }

    /**
     * Returns a boolean indicating if Elasticsearch is deployed in a container (such as Docker). The way
     * we determine if Elasticsearch is deployed in a container is by reading the ELASTIC_CONTAINER env
     * variable. This should be set to true if Elasticsearch is in a container.
     *
     * @return if elasticsearch is running in a container
     */
    public boolean inContainer() {
        return inContainer;
    }

    /**
     * Returns the hash for the bootstrap password. This is the password passed as an environmental variable
     * for use when elasticsearch is deployed in a container.
     *
     * @return the password hash
     */
    public char[] getPasswordHash() {
        return passwordHash;
    }

    public static ContainerSettings parseAndCreate() {
        String inContainerString = System.getenv(CONTAINER_ENV_VAR);
        boolean inContainer = inContainerString != null && Booleans.parseBoolean(inContainerString);
        char[] passwordHash;

        String passwordString = System.getenv(BOOTSTRAP_PASSWORD_ENV_VAR);
        if (passwordString != null) {
            SecureString password = new SecureString(passwordString.toCharArray());
            passwordHash = Hasher.BCRYPT.hash(password);
            password.close();
        } else {
            passwordHash = null;
        }

        return new ContainerSettings(inContainer, passwordHash);
    }
}
