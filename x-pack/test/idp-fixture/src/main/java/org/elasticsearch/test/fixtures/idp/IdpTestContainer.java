/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.fixtures.idp;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.Path;

import static org.elasticsearch.test.fixtures.ResourceUtils.copyResourceToFile;

public final class IdpTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final String DOCKER_BASE_IMAGE = "docker.elastic.co/elasticsearch-dev/idp-fixture:1.0";
    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Path certsPath;

    /**
     * for packer caching only
     * */
    protected IdpTestContainer() {
        this(Network.newNetwork());
    }

    public IdpTestContainer(Network network) {
        super(
            new ImageFromDockerfile("es-idp-testfixture").withDockerfileFromBuilder(builder -> builder.from(DOCKER_BASE_IMAGE).build())
                .withFileFromClasspath("idp/jetty-custom/ssl.mod", "/idp/jetty-custom/ssl.mod")
                .withFileFromClasspath("idp/jetty-custom/keystore", "/idp/jetty-custom/keystore")
                .withFileFromClasspath("idp/shib-jetty-base/", "/idp/shib-jetty-base/")
                .withFileFromClasspath("idp/shibboleth-idp/", "/idp/shibboleth-idp/")
                .withFileFromClasspath("idp/bin/", "/idp/bin/")
        );
        withNetworkAliases("idp");
        withNetwork(network);
        waitingFor(Wait.forListeningPorts(4443));
        addExposedPorts(4443, 8443);
    }

    @Override
    public void stop() {
        super.stop();
        temporaryFolder.delete();
    }

    public Path getBrowserPem() {
        try {
            temporaryFolder.create();
            certsPath = temporaryFolder.newFolder("certs").toPath();
            return copyResourceToFile(getClass(), certsPath, "idp/shibboleth-idp/credentials/idp-browser.pem");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Integer getDefaultPort() {
        return getMappedPort(4443);
    }
}
