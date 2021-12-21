/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis;

import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.IgnoreEmptyDirectories;

/**
 * This implementation is used to fix gradle 8 compatibility of
 * the CheckForbiddenApis task which is built with gradle 4 support
 * in mind.
 * */
public class CheckForbiddenApisTask extends CheckForbiddenApis {

    /**
     * Add additional annotation to make this input gradle 8 compliant.
     * Otherwise we see a deprecation warning here starting with gradle 7.4
     * */
    @Override
    @IgnoreEmptyDirectories
    public FileTree getClassFiles() {
        return super.getClassFiles();
    }
}
