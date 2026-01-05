/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.fixtures.krb5kdc;

import com.github.dockerjava.api.model.ExposedPort;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public final class Krb5kDcContainer extends DockerEnvironmentAwareTestContainer {
    public static final String DOCKER_BASE_IMAGE = "docker.elastic.co/elasticsearch-dev/krb5dc-fixture:1.1";
    public static final int KDC_PORT = 88;
    public static final int KDC_ADMIN_PORT = 4444;

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final ProvisioningId provisioningId;
    private Path krb5ConfFile;
    private Path keytabFile;
    private Path esKeytabFile;

    public enum ProvisioningId {
        HDFS(
            "hdfs",
            "/fixture/provision/hdfs.sh",
            "/fixture/build/keytabs/hdfs_hdfs.build.elastic.co.keytab",
            "/fixture/build/keytabs/elasticsearch.keytab",
            "hdfs/hdfs.build.elastic.co@BUILD.ELASTIC.CO"
        ),
        PEPPA(
            "peppa",
            "/fixture/provision/peppa.sh",
            "/fixture/build/keytabs/peppa.keytab",
            "/fixture/build/keytabs/HTTP_localhost.keytab",
            "peppa@BUILD.ELASTIC.CO"
        );

        private final String id;
        private final String scriptPath;
        private final String keytabPath;
        public final String esKeytab;
        private final String keytabPrincipal;

        ProvisioningId(String id, String scriptPath, String keytabPath, String esKeytab, String principal) {
            this.id = id;
            this.scriptPath = scriptPath;
            this.keytabPath = keytabPath;
            this.esKeytab = esKeytab;
            this.keytabPrincipal = principal;
        }
    }

    public Krb5kDcContainer() {
        this(ProvisioningId.HDFS);
    }

    public Krb5kDcContainer(ProvisioningId provisioningId) {
        super(new RemoteDockerImage(DOCKER_BASE_IMAGE));
        this.provisioningId = provisioningId;
        addExposedPorts(KDC_PORT, KDC_ADMIN_PORT);
        withStartupTimeout(Duration.ofMinutes(2));
        withCreateContainerCmdModifier(cmd -> {
            // Expose UDP port using --expose with -P flag approach
            // This works around Docker Desktop macOS bug with random UDP port bindings
            // See: https://github.com/docker/for-mac/issues/7754
            // Note: publishAllPorts is required; direct port binding with Ports.Binding.empty() doesn't work
            List<ExposedPort> exposedPorts = new ArrayList<>();
            for (ExposedPort p : cmd.getExposedPorts()) {
                exposedPorts.add(p);
            }
            exposedPorts.add(ExposedPort.udp(KDC_PORT));
            cmd.withExposedPorts(exposedPorts);
            // Enable publish all ports (-P flag) - only publishes ports that are exposed
            // The krb5dc-fixture image only exposes port 88 (TCP+UDP), so this is safe
            cmd.withPublishAllPorts(true);
        });
        // HostPortWaitStrategy doesn't support UDP ports, so wait for TCP ports only
        setWaitStrategy(Wait.forListeningPorts(KDC_PORT, KDC_ADMIN_PORT).withStartupTimeout(Duration.ofMinutes(2)));
        withNetworkAliases("kerberos.build.elastic.co", "build.elastic.co");
        withCopyFileToContainer(MountableFile.forHostPath("/dev/urandom"), "/dev/random");
        withExtraHost("kerberos.build.elastic.co", "127.0.0.1");
        withCommand("sh", provisioningId.scriptPath);
    }

    @Override
    public void start() {
        try {
            temporaryFolder.create();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        super.start();
        System.setProperty("java.security.krb5.conf", getConfPath().toString());
    }

    @Override
    public void stop() {
        super.stop();
        System.clearProperty("java.security.krb5.conf");
        temporaryFolder.delete();
    }

    @SuppressWarnings("unchecked")
    public String getConf() {
        // Retrieve the dynamically assigned UDP port
        var portBindings = getCurrentContainerInfo().getNetworkSettings().getPorts().getBindings();
        if (portBindings == null) {
            throw new IllegalStateException(
                "Port bindings are null - container may not be properly started. "
                    + "Ensure the container has been started before calling getConf(). Container ID: "
                    + getContainerId()
            );
        }
        var udpBindings = portBindings.get(ExposedPort.udp(KDC_PORT));
        if (udpBindings == null || udpBindings.length == 0) {
            throw new IllegalStateException(
                "UDP port "
                    + KDC_PORT
                    + " binding not found - container may not have properly exposed the UDP port. "
                    + "This may indicate a Docker Desktop configuration issue or the container failed to start properly. "
                    + "Container ID: "
                    + getContainerId()
            );
        }
        String hostPortSpec = udpBindings[0].getHostPortSpec();
        String s = copyFileFromContainer("/fixture/build/krb5.conf.template", i -> IOUtils.toString(i, StandardCharsets.UTF_8));
        return s.replace("#KDC_DOCKER_HOST", "kdc = 127.0.0.1:" + hostPortSpec);
    }

    public Path getKeytab() {
        if (keytabFile != null) {
            return keytabFile;
        }
        try {
            String keytabPath = provisioningId.keytabPath;
            keytabFile = temporaryFolder.newFile(provisioningId.id + ".keytab").toPath();
            copyFileFromContainer(keytabPath, keytabFile.toAbsolutePath().toString());
            return keytabFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path getEsKeytab() {
        if (esKeytabFile != null) {
            return esKeytabFile;
        }
        try {
            esKeytabFile = temporaryFolder.newFile("elasticsearch.keytab").toPath();
            copyFileFromContainer(provisioningId.esKeytab, esKeytabFile.toAbsolutePath().toString());
            return esKeytabFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Path getConfPath() {
        if (krb5ConfFile != null) {
            return krb5ConfFile;
        }
        try {
            krb5ConfFile = temporaryFolder.newFile("krb5.conf").toPath();
            Files.writeString(krb5ConfFile, getConf());
            return krb5ConfFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getPrincipal() {
        return provisioningId.keytabPrincipal;
    }

    public String getEsPrincipal() {
        return "elasticsearch@BUILD.ELASTIC.CO";
    }
}
