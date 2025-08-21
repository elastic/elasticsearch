/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.smb;

import com.github.dockerjava.api.model.Capability;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.images.RemoteDockerImage;

import java.time.Duration;

public final class SmbTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final String DOCKER_BASE_IMAGE = "docker.elastic.co/elasticsearch-dev/es-smb-fixture:1.0";
    public static final int AD_LDAP_PORT = 636;
    public static final int AD_LDAP_GC_PORT = 3269;

    public SmbTestContainer() {
        super(new RemoteDockerImage(DOCKER_BASE_IMAGE));

        addExposedPort(AD_LDAP_PORT);
        addExposedPort(AD_LDAP_GC_PORT);

        setWaitStrategy(
            new WaitAllStrategy().withStartupTimeout(Duration.ofSeconds(120))
                .withStrategy(Wait.forLogMessage(".*Samba started.*", 1))
                .withStrategy(Wait.forListeningPort())
        );

        getCreateContainerCmdModifiers().add(createContainerCmd -> {
            createContainerCmd.getHostConfig().withCapAdd(Capability.SYS_ADMIN);
            return createContainerCmd;
        });
    }

    public String getAdLdapUrl() {
        return "ldaps://localhost:" + getMappedPort(AD_LDAP_PORT);
    }

    public String getAdLdapGcUrl() {
        return "ldaps://localhost:" + getMappedPort(AD_LDAP_GC_PORT);
    }
}
