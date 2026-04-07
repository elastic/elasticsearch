/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
