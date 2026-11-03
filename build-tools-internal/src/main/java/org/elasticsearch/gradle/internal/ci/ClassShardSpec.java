/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.ci;

import org.gradle.api.file.FileTreeElement;
import org.gradle.api.specs.Spec;

import java.io.Serializable;
import java.util.Set;

/**
 * Excludes the test class files that belong to other shards of a test task split by class. Nested classes
 * follow their top-level class. A plain record rather than a lambda so the configuration cache can store it.
 *
 * @param count  the number of shards of the task
 * @param shards the shards to keep
 */
record ClassShardSpec(int count, Set<Integer> shards) implements Spec<FileTreeElement>, Serializable {

    @Override
    public boolean isSatisfiedBy(FileTreeElement element) {
        if (element.isDirectory() || element.getName().endsWith(".class") == false) {
            return false;
        }
        String className = CiShards.topLevelClassName(element.getRelativePath().getPathString());
        return shards.contains(CiShards.bucket(className, count)) == false;
    }
}
