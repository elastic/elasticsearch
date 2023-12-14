/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.smb;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class SmbTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final String DOCKER_BASE_IMAGE = "ubuntu:16.04";

    public SmbTestContainer() {

        // FROM ubuntu:16.04
        // RUN apt-get update -qqy && apt-get install -qqy samba ldap-utils
        // ADD . /fixture
        // RUN chmod +x /fixture/src/main/resources/provision/installsmb.sh
        // RUN /fixture/src/main/resources/provision/installsmb.sh
        //
        // EXPOSE 389
        // EXPOSE 636
        // EXPOSE 3268
        // EXPOSE 3269
        //
        // CMD service samba-ad-dc restart && sleep infinity
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
        addExposedPort(389);
        addExposedPort(636);
        addExposedPort(3268);
        addExposedPort(3269);
    }

    public String getAdLdapUrl() {
        return "ldaps://localhost:" + getMappedPort(636);
    }

    public String getAdLdapGcUrl() {
        return "ldaps://localhost:" + getMappedPort(3269);
    }
}
