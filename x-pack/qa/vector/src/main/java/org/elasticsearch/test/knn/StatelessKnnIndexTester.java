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
import java.util.logging.Logger;

/**
 * Thin wrapper around {@link KnnIndexTester} that registers the stateless
 * directory type so that {@code "directory_type": "stateless"} works in
 * the test configuration JSON.
 *
 * <p>Run via: {@code ./gradlew :modules-self-managed:vector:checkVec --args="config.json"}
 */
public class StatelessKnnIndexTester {

    private static final Logger logger = Logger.getLogger(StatelessKnnIndexTester.class.getName());
    private static final String DISKSTATS_DEVICE = System.getProperty("bench.device", "nvme1n1");

    public static void main(String[] args) throws Exception {
        boolean shared = true;
        boolean prewarm = true;
        KnnIndexTester.registerDirectoryType(
            "stateless",
            StatelessKnnIndexTester::newStatelessDirectory,
            shared,
            prewarm,
            StatelessKnnIndexTester::logDiagnostics
        );
        KnnIndexTester.main(args);
    }

    static Directory newStatelessDirectory(Path indexPath) throws IOException {
        Path workPath = indexPath.resolveSibling(indexPath.getFileName() + ".stateless_work");
        Files.createDirectories(workPath);
        return StatelessDirectoryFactory.create(indexPath, workPath);
    }

    private static void logDiagnostics(Directory dir, String label) {
        StatelessDirectoryFactory.logCacheStats(dir, label);
        logDiskStats(label);
    }

    private static void logDiskStats(String label) {
        try {
            for (String line : Files.readAllLines(Path.of("/proc/diskstats"))) {
                String[] fields = line.trim().split("\\s+");
                if (fields.length >= 6 && fields[2].equals(DISKSTATS_DEVICE)) {
                    logger.info(
                        String.format("DISKSTATS[%s] device=%s reads=%s sectors_read=%s", label, DISKSTATS_DEVICE, fields[3], fields[5])
                    );
                    return;
                }
            }
        } catch (IOException e) {
            logger.warning("Failed to read /proc/diskstats: " + e.getMessage());
        }
    }
}
