/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.rewrite;

import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.workers.WorkParameters;

import java.io.File;

public interface RewriteParameters extends WorkParameters {

    ListProperty<String> getActiveRecipes();
    ListProperty<String> getActiveStyles();

    ListProperty<File> getAllJavaPaths();
    ListProperty<File> getAllDependencyPaths();
    RegularFileProperty getProjectDirectory();
    RegularFileProperty getConfigFile();
}
