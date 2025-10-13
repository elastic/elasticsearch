/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.testfixtures;

import org.gradle.api.Named;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;

import java.io.File;

public abstract class TestFixtureDeployment implements Named {

    private final String name;

    public TestFixtureDeployment(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    public abstract Property<String> getDockerRegistry();

    public abstract Property<File> getDockerContext();

    public abstract Property<String> getVersion();

    public abstract ListProperty<String> getBaseImages();
}
