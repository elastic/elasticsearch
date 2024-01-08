/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.smb;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

public final class SmbTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final String DOCKER_BASE_IMAGE = "ubuntu:16.04";
    public static final int AD_LDAP_PORT = 636;
    public static final int AD_LDAP_GC_PORT = 3269;

    public SmbTestContainer() {
        super(
            new ImageFromDockerfile("es-smb-fixture").withDockerfileFromBuilder(
                builder -> builder.from(DOCKER_BASE_IMAGE)
                    .run("apt-get update -qqy && apt-get install -qqy samba ldap-utils")
                    .copy("fixture/provision/installsmb.sh", "/fixture/provision/installsmb.sh")
                    .copy("fixture/certs/ca.key", "/fixture/certs/ca.key")
                    .copy("fixture/certs/ca.pem", "/fixture/certs/ca.pem")
                    .copy("fixture/certs/cert.pem", "/fixture/certs/cert.pem")
                    .copy("fixture/certs/key.pem", "/fixture/certs/key.pem")
                    .run("chmod +x /fixture/provision/installsmb.sh")
                    .run("/fixture/provision/installsmb.sh")
                    .cmd("service samba-ad-dc restart && sleep infinity")
                    .build()
            )
                .withFileFromClasspath("fixture/provision/installsmb.sh", "/smb/provision/installsmb.sh")
                .withFileFromClasspath("fixture/certs/ca.key", "/smb/certs/ca.key")
                .withFileFromClasspath("fixture/certs/ca.pem", "/smb/certs/ca.pem")
                .withFileFromClasspath("fixture/certs/cert.pem", "/smb/certs/cert.pem")
                .withFileFromClasspath("fixture/certs/key.pem", "/smb/certs/key.pem")
        );
        // addExposedPort(389);
        // addExposedPort(3268);
        addExposedPort(AD_LDAP_PORT);
        addExposedPort(AD_LDAP_GC_PORT);
    }

    public String getAdLdapUrl() {
        return "ldaps://localhost:" + getMappedPort(AD_LDAP_PORT);
    }

    public String getAdLdapGcUrl() {
        return "ldaps://localhost:" + getMappedPort(AD_LDAP_GC_PORT);
    }
}
