/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.fixtures.testcontainers;

import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static org.elasticsearch.test.fixtures.testcontainers.DockerAvailability.assumeDockerIsAvailable;

/**
 * Base class for JUnit 4 {@code @ClassRule} fixtures that orchestrate one or more Docker containers
 * sharing a single {@link Network}.
 *
 * <p>The lifecycle managed by {@link #apply} is:
 * <ol>
 *   <li>Call {@link DockerAvailability#assumeDockerIsAvailable()} — skips or fails the suite if
 *       Docker is not present, <em>before</em> allocating any resources.</li>
 *   <li>Create a temporary directory that is bind-mounted into each Elasticsearch container as
 *       {@code /tmp/es-repo}.</li>
 *   <li>Create a {@link Network} and delegate to {@link #buildRuleChain} so that subclasses can
 *       create their containers and wire them together.</li>
 *   <li>In the {@code finally} block, call {@link #clearContainerReferences()} and then
 *       {@link #deleteRecursively} on the temp directory.</li>
 * </ol>
 *
 * <p>Subclasses implement two template methods:
 * <ul>
 *   <li>{@link #buildRuleChain(Network, Path)} — create containers, save references to fields, and
 *       return a {@link RuleChain} whose outermost rule is
 *       {@code RuleChain.outerRule(Junit4NetworkRule.from(network))}.</li>
 *   <li>{@link #clearContainerReferences()} — null out the container fields saved in
 *       {@code buildRuleChain} so that callers cannot accidentally read stale state after the rule
 *       has finished.</li>
 * </ul>
 */
public abstract class AbstractDockerClusterRule implements TestRule {

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                // Use assumeDockerIsAvailable() (not a plain Assume.assumeTrue) so that the rule
                // also applies the dockerOnLinuxExclusions list and asserts hard on CI when probing fails.
                // Calling it BEFORE allocating the temp dir avoids leaking the dir on assumption failure.
                assumeDockerIsAvailable();
                Path repoDir = Files.createTempDirectory("es-docker-repo");
                try {
                    // Network is lazy in Testcontainers (no docker resource until first getId()), so allocating
                    // it here costs nothing if the rule chain below short-circuits.
                    Network network = Network.newNetwork();
                    buildRuleChain(network, repoDir).apply(base, description).evaluate();
                } finally {
                    clearContainerReferences();
                    deleteRecursively(repoDir);
                }
            }
        };
    }

    /**
     * Creates all containers for this cluster fixture, stores references to them in instance
     * fields (so that address-getter methods can read them during the test run), and returns a
     * {@link RuleChain} that starts and stops the containers in order.
     *
     * <p>The outermost rule in the chain must be
     * {@code RuleChain.outerRule(Junit4NetworkRule.from(network))} so that the Docker network is
     * cleaned up after the last container stops.
     *
     * @param network the shared Docker network; all containers must be attached to it
     * @param repoDir temp directory to bind-mount as {@code /tmp/es-repo} in each ES container
     * @return a fully configured rule chain; must not be {@code null}
     */
    protected abstract RuleChain buildRuleChain(Network network, Path repoDir);

    /**
     * Nulls out any container references saved by {@link #buildRuleChain}. Called unconditionally
     * in the {@code finally} block of {@link #apply}, after the test run completes (or fails).
     * Prevents callers from reading stale host/port data after the rule scope ends.
     */
    protected abstract void clearContainerReferences();

    /**
     * Wraps a {@link GenericContainer} in a minimal {@link TestRule} that starts the container
     * before the inner statement and stops it afterwards.
     *
     * @param container the container to manage
     * @return a {@link TestRule} suitable for use with {@link RuleChain#around}
     */
    protected static TestRule asRule(GenericContainer<?> container) {
        return (base, description) -> new Statement() {
            @Override
            public void evaluate() throws Throwable {
                container.start();
                try {
                    base.evaluate();
                } finally {
                    container.stop();
                }
            }
        };
    }

    /**
     * Recursively deletes {@code root} and all files beneath it, ignoring individual failures.
     *
     * <p>Individual failures are silently skipped rather than aborting the walk because Docker
     * bind-mount releases are asynchronous: a file may briefly be unreadable right after the
     * container stops, and aborting would leak the rest of the tree under {@code /tmp}.
     *
     * @param root the directory tree to delete; silently does nothing if it does not exist
     */
    protected static void deleteRecursively(Path root) throws IOException {
        if (Files.exists(root) == false) {
            return;
        }
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.deleteIfExists(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
