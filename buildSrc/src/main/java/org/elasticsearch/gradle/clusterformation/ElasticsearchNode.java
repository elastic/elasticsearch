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
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MINUTES;

public class ElasticsearchNode implements ElasticsearchConfiguration {

    private static final ExecutorService threadPool = Executors.newCachedThreadPool();
    private static final String PIDFILE_NAME = "node.pid";

    private final Logger logger = Logging.getLogger(ElasticsearchNode.class);

    private final String name;
    private final GradleServicesAdapter services;
    private final AtomicInteger noOfClaims = new AtomicInteger();
    private final File sharedArtifactsDir;
    private final File workDir;
    private final File pidFile;


    private Distribution distribution;
    private Version version;
    private volatile Future<?> runner = null;

    public ElasticsearchNode(String name, GradleServicesAdapter services, File sharedArtifactsDir, File workDirBase) {
        this.name = name;
        this.services = services;
        this.sharedArtifactsDir = sharedArtifactsDir;
        this.workDir = new File(workDirBase, name);
        this.pidFile = new File(workDir, "node.pid");
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
    public synchronized Future<?> start() {
        if (runner != null) {
            logger.lifecycle("Already started cluster: {}", name);
            return runner;
        } else {
            logger.lifecycle("Starting cluster: {}", name);
        }
        File artifact = ClusterformationPlugin.getArtifact(sharedArtifactsDir, getDistribution(), getVersion());
        if (artifact.exists() == false) {
            throw new ClusterFormationException("Can not start node, missing artifact: " + artifact);
        }

        runner = threadPool.submit(() -> {
            services.sync(copySpec -> {
                if (getDistribution().getExtension() == "zip") {
                    copySpec.from(services.zipTree(artifact));
                } else {
                    throw new ClusterFormationException("Only ZIP distributions are supported for now");
                }
                copySpec.into(workDir);
            });
            // For some reason the pid file is not created in the node dir, but one level up
            exec(getDistroPath("bin/elasticsearch"));
        });
        return runner;
    }

    public String getDistroPath(String pathTo) {
        return getDistribution().getFileName() + "-" + getVersion() + "/" + pathTo;
    }

    /**
     * Stops a running cluster if it's not claimed. Does nothing otherwise.
     */
    @Override
    public synchronized void unClaimAndStop() {
        int decrementedClaims = noOfClaims.decrementAndGet();
        if (decrementedClaims > 0) {
            logger.lifecycle("Not stopping {}, since cluster still has {} claim(s)", name, decrementedClaims);
            return;
        }
        if (runner == null) {
            logger.lifecycle("Asked to unClaimAndStop, but cluster was not running: {}", name);
            return;
        }
        logger.lifecycle("Stopping {}, number of claims is {}", name, decrementedClaims);

        runner.cancel(true);
        try {
            runner.get(1, MINUTES);
        } catch (ExecutionException e) {
            throw new ClusterFormationException("Exception while starting node `" + getName() + "`.", e);
        } catch (TimeoutException e) {
            throw new ClusterFormationException("Timed out while waiting for node to stop `" + getName() + "`.", e);
        } catch (CancellationException e) {
            logger.info("Cancelled runner so it would stop!", e);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for runner", e);
            Thread.currentThread().interrupt();
        }
    }

    public void exec(String executable, String... args) {
        logger.lifecycle("Running {} args: {} in `{}`", executable, args, workDir);
        services.exec(spec -> {
            spec.setExecutable(executable);
            spec.setArgs(Arrays.asList(args));
            spec.setWorkingDir(workDir);
        }).assertNormalExitValue();
    }

    private synchronized void checkNotRunning() {
        if (runner != null) {
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
