/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.clusterformation;

import org.elasticsearch.GradleServicesAdapter;
import org.elasticsearch.gradle.Distribution;
import org.elasticsearch.gradle.Version;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.gradle.internal.os.OperatingSystem.*;

public class ElasticsearchNode implements ElasticsearchConfiguration {

    public static final int ES_DESTROY_TIMEOUT = 20;
    public static final TimeUnit ES_DESTROY_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private final Logger logger = Logging.getLogger(ElasticsearchNode.class);

    private final String name;
    private final GradleServicesAdapter services;
    private final AtomicInteger noOfClaims = new AtomicInteger();
    private final File sharedArtifactsDir;
    private final File workDir;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private Distribution distribution;
    private Version version;
    private Process esProcess;

    public ElasticsearchNode(String name, GradleServicesAdapter services, File sharedArtifactsDir, File workDirBase) {
        this.name = name;
        this.services = services;
        this.sharedArtifactsDir = sharedArtifactsDir;
        this.workDir = new File(workDirBase, name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Version getVersion() {
        return version;
    }

    @Override
    public void setVersion(Version version) {
        checkNotRunning();
        this.version = version;
    }

    @Override
    public Distribution getDistribution() {
        return distribution;
    }

    @Override
    public void setDistribution(Distribution distribution) {
        checkNotRunning();
        this.distribution = distribution;
    }

    @Override
    public void claim() {
        noOfClaims.incrementAndGet();
    }

    /**
     * Start the cluster if not running. Does nothing if the cluster is already running.
     *
     * @return future of thread running in the background
     */
    @Override
    public void start() {
        if (started.getAndSet(true)) {
            logger.lifecycle("Already started cluster: {}", name);
            return;
        } else {
            logger.lifecycle("Starting cluster: {}", name);
        }
        File artifact = ClusterformationPlugin.getArtifact(sharedArtifactsDir, getDistribution(), getVersion());
        if (artifact.exists() == false) {
            throw new ClusterFormationException("Can not start node, missing artifact: " + artifact);
        }

        services.sync(copySpec -> {
            if (getDistribution().getExtension() == "zip") {
                copySpec.from(services.zipTree(artifact));
            } else {
                throw new ClusterFormationException("Only ZIP distributions are supported for now");
            }
            copySpec.into(workDir);
        });

        logger.info("Running `{}` in `{}`", getDistroPath("bin/elasticsearch"), workDir);
        if (current().isWindows()) {
            // TODO
            logger.lifecycle("Windows is not supported at this time");
        } else {
            try {
                esProcess = new ProcessBuilder(getDistroPath("bin/elasticsearch"))
                    .directory(workDir)
                    .start();
            } catch (IOException e) {
                throw new ClusterFormationException("Failed to start ES process", e);
            }
        }
    }

    private String getDistroPath(String pathTo) {
        return getDistribution().getFileName() + "-" + getVersion() + "/" + pathTo;
    }

    /**
     * Stops a running cluster if it's not claimed. Does nothing otherwise.
     */
    @Override
    public void unClaimAndStop() {
        int decrementedClaims = noOfClaims.decrementAndGet();
        if (decrementedClaims > 0) {
            logger.lifecycle("Not stopping {}, since cluster still has {} claim(s)", name, decrementedClaims);
            return;
        }
        if (started.getAndSet(false) == false) {
            logger.lifecycle("Asked to unClaimAndStop, but cluster was not running: {}", name);
            return;
        }
        logger.lifecycle("Stopping {}, number of claims is {}", name, decrementedClaims);
        doStop();
    }

    @Override
    public void forceStop() {
        logger.lifecycle("Forcefully stopping {}, number of claims is {}", name, noOfClaims.get());
        doStop();
    }

    private void doStop() {
        if (current().isWindows()) {
            return;
        }
        logProcessInfo("Self:", esProcess.info());
        esProcess.children().forEach( child -> {
            logProcessInfo("Cluster Child:", child.info());
        });

        if (esProcess.isAlive() == false) {
            throw new ClusterFormationException("Cluster `" + name + "` wasn't alive when we tried to destroy it");
        }
        esProcess.destroy();
        waitForESProcess();
        if(esProcess.isAlive()) {
            logger.info("Cluster `{}` did not terminate after {} {}, stopping it forcefully",
                name, ES_DESTROY_TIMEOUT, ES_DESTROY_TIMEOUT_UNIT
            );
            esProcess.destroyForcibly();
        }
        waitForESProcess();
        if (esProcess.isAlive()) {
            throw new ClusterFormationException("Was not able to terminate cluster: " + name);
        }
    }

    private void logProcessInfo(String prefix, ProcessHandle.Info info) {
        logger.lifecycle(prefix + " commandLine:`{}` command:`{}` args:`{}`",
            info.commandLine().orElse("-"), info.command().orElse("-"),
            Arrays.stream(info.arguments().orElse(new String[] {}))
                .map(each -> "'"+each+"'")
                .collect(Collectors.joining(", "))
        );
    }

    private void waitForESProcess() {
        try {
            esProcess.waitFor(ES_DESTROY_TIMEOUT, ES_DESTROY_TIMEOUT_UNIT);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void checkNotRunning() {
        if (started.get() == true) {
            throw new IllegalStateException("Configuration can not be altered while running ");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticsearchNode that = (ElasticsearchNode) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
