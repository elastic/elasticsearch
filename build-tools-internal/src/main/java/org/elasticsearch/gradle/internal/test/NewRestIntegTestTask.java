/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.gradle.api.Action;
import org.gradle.api.file.FileTree;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.util.PatternFilterable;

public abstract class NewRestIntegTestTask extends Test {

    public NewRestIntegTestTask() {
        dependsOn(getDistribution());
    }

    @Internal
    public abstract Property<ElasticsearchDistribution> getDistribution();

    @Classpath
    public FileTree getDistributionClasspath() {
        return getDistributionFiles(filter -> filter.include("**/*.jar"));
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileTree getDistributionFiles() {
        return getDistributionFiles(filter -> filter.exclude("**/*.jar"));
    }

    private FileTree getDistributionFiles(Action<PatternFilterable> patternFilter) {
        return getDistribution().get().getExtracted().getAsFileTree().matching(patternFilter);
    }
}
