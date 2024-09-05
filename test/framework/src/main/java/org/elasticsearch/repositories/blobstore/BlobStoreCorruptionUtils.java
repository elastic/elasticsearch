/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.tests.mockfile.ExtrasFS;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Base64;

import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThan;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThanMany;

public class BlobStoreCorruptionUtils {
    private static final Logger logger = LogManager.getLogger(BlobStoreCorruptionUtils.class);

    public static Path corruptRandomFile(Path repositoryRootPath) throws IOException {
        final var corruptedFileType = getRandomCorruptibleFileType();
        final var corruptedFile = getRandomFileToCorrupt(repositoryRootPath, corruptedFileType);
        if (randomBoolean()) {
            logger.info("--> deleting [{}]", corruptedFile);
            Files.delete(corruptedFile);
        } else {
            corruptFileContents(corruptedFile);
        }
        return corruptedFile;
    }

    public static void corruptFileContents(Path fileToCorrupt) throws IOException {
        final var oldFileContents = Files.readAllBytes(fileToCorrupt);
        logger.info("--> contents of [{}] before corruption: [{}]", fileToCorrupt, Base64.getEncoder().encodeToString(oldFileContents));
        final byte[] newFileContents = new byte[randomBoolean() ? oldFileContents.length : between(0, oldFileContents.length)];
        System.arraycopy(oldFileContents, 0, newFileContents, 0, newFileContents.length);
        if (newFileContents.length == oldFileContents.length) {
            final var corruptionPosition = between(0, newFileContents.length - 1);
            newFileContents[corruptionPosition] = randomValueOtherThan(oldFileContents[corruptionPosition], ESTestCase::randomByte);
            logger.info(
                "--> updating byte at position [{}] from [{}] to [{}]",
                corruptionPosition,
                oldFileContents[corruptionPosition],
                newFileContents[corruptionPosition]
            );
        } else {
            logger.info("--> truncating file from length [{}] to length [{}]", oldFileContents.length, newFileContents.length);
        }
        Files.write(fileToCorrupt, newFileContents);
        logger.info("--> contents of [{}] after corruption: [{}]", fileToCorrupt, Base64.getEncoder().encodeToString(newFileContents));
    }

    public static RepositoryFileType getRandomCorruptibleFileType() {
        return randomValueOtherThanMany(
            // these blob types do not have reliable corruption detection, so we must skip them
            t -> t == RepositoryFileType.ROOT_INDEX_N || t == RepositoryFileType.ROOT_INDEX_LATEST,
            () -> randomFrom(RepositoryFileType.values())
        );
    }

    public static Path getRandomFileToCorrupt(Path repositoryRootPath, RepositoryFileType corruptedFileType) throws IOException {
        final var corruptibleFiles = new ArrayList<Path>();
        Files.walkFileTree(repositoryRootPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs) throws IOException {
                if (ExtrasFS.isExtra(filePath.getFileName().toString()) == false
                    && RepositoryFileType.getRepositoryFileType(repositoryRootPath, filePath) == corruptedFileType) {
                    corruptibleFiles.add(filePath);
                }
                return super.visitFile(filePath, attrs);
            }
        });
        return randomFrom(corruptibleFiles);
    }
}
