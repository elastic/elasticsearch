/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local.distribution;

import org.elasticsearch.test.cluster.util.Version;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A {@link DistributionResolver} for resolving locally built distributions for the current version of Elasticsearch.
 */
public class LocalDistributionResolver implements DistributionResolver {
    private final DistributionResolver delegate;

    public LocalDistributionResolver(DistributionResolver delegate) {
        this.delegate = delegate;
    }

    @Override
    public DistributionDescriptor resolve(Version version, DistributionType type) {
        if (version.equals(Version.CURRENT)) {
            String testDistributionPath = System.getProperty(type.getSystemProperty());

            if (testDistributionPath == null) {
                if (type == DistributionType.DEFAULT) {
                    String taskPath = System.getProperty("tests.task");
                    String project = taskPath.substring(0, taskPath.lastIndexOf(':'));
                    String taskName = taskPath.substring(taskPath.lastIndexOf(':') + 1);

                    throw new IllegalStateException(
                        "Cannot locate Elasticsearch distribution. Ensure you've added the following to the build script for project '"
                            + project
                            + "':\n\n"
                            + "tasks.named('"
                            + taskName
                            + "') {\n"
                            + "  usesDefaultDistribution()\n"
                            + "}"
                    );
                } else {
                    throw new IllegalStateException(
                        "Cannot locate Elasticsearch distribution. The '" + type.getSystemProperty() + "' system property is null."
                    );
                }
            }

            Path testDistributionDir = Path.of(testDistributionPath);
            if (Files.notExists(testDistributionDir)) {
                throw new IllegalStateException(
                    "Cannot locate Elasticsearch distribution. Directory at '" + testDistributionDir + "' does not exist."
                );
            }

            return new DefaultDistributionDescriptor(
                version,
                Boolean.parseBoolean(System.getProperty("build.snapshot", "true")),
                testDistributionDir,
                type
            );
        }

        return delegate.resolve(version, type);
    }
}
