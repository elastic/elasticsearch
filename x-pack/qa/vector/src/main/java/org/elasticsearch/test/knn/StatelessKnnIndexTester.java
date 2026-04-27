/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.knn;

import org.apache.lucene.store.Directory;
import org.elasticsearch.xpack.stateless.lucene.StatelessDirectoryFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Thin wrapper around {@link KnnIndexTester} that registers the stateless
 * directory type so that {@code "directory_type": "stateless"} works in
 * the test configuration JSON.
 *
 * <p>Run via: {@code ./gradlew :modules-self-managed:vector:checkVec --args="config.json"}
 */
public class StatelessKnnIndexTester {

    public static void main(String[] args) throws Exception {
        boolean shared = true; // shared for both read and write
        boolean prewarm = true;
        KnnIndexTester.registerDirectoryType(
            "stateless",
            StatelessKnnIndexTester::newStatelessDirectory,
            shared,
            prewarm,
            StatelessDirectoryFactory::logCacheStats
        );
        KnnIndexTester.main(args);
    }

    static Directory newStatelessDirectory(Path indexPath) throws IOException {
        Path workPath = indexPath.resolveSibling(indexPath.getFileName() + ".stateless_work");
        Files.createDirectories(workPath);
        return StatelessDirectoryFactory.create(indexPath, workPath);
    }
}
