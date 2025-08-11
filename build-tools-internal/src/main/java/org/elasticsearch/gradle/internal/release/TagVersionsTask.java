/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.options.Option;

import java.util.List;

/**
 * This is a no-op task that is only implemented for v8+
 */
public class TagVersionsTask extends DefaultTask {

    @Option(option = "release", description = "Dummy option")
    public void release(String version) {}

    @Option(option = "tag-version", description = "Dummy option")
    public void tagVersions(List<String> version) {}
}
