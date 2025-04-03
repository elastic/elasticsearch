/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test.rest;

import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.internal.tasks.TaskDependencyFactory;
import org.gradle.api.plugins.jvm.internal.DefaultJvmTestSuite;
import org.gradle.api.tasks.SourceSetContainer;

public abstract class DefaultJavaRestTestSuite extends DefaultJvmTestSuite implements JavaRestTestSuite {
    public DefaultJavaRestTestSuite(
        String name,
        SourceSetContainer sourceSets,
        ConfigurationContainer configurations,
        TaskDependencyFactory taskDependencyFactory
    ) {
        super(name, sourceSets, configurations, taskDependencyFactory);
    }
}
