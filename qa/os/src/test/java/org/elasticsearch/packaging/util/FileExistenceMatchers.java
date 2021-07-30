/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class FileExistenceMatchers {

    private static class FileExistsMatcher extends TypeSafeMatcher<Path> {

        @Override
        protected boolean matchesSafely(final Path path) {
            return Files.exists(path);
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("expected path to exist");
        }

        @Override
        protected void describeMismatchSafely(final Path path, final Description mismatchDescription) {
            mismatchDescription.appendText("path " + path + " does not exist");
        }

    }

    public static FileExistsMatcher fileExists() {
        return new FileExistsMatcher();
    }

    private static class FileDoesNotExistMatcher extends TypeSafeMatcher<Path> {

        @Override
        protected boolean matchesSafely(final Path path) {
            return Files.exists(path) == false;
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("expected path to not exist");
        }

        @Override
        protected void describeMismatchSafely(final Path path, final Description mismatchDescription) {
            if (Files.isDirectory(path)) {
                mismatchDescription.appendText("path " + path + " is a directory with contents\n");
                try {
                    Files.walkFileTree(path, new SimpleFileVisitor<>() {

                        @Override
                        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                            mismatchDescription.appendValue(path.relativize(file)).appendText("\n");
                            return FileVisitResult.CONTINUE;
                        }

                    });
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else {
                mismatchDescription.appendText("path " + path + " exists");
            }
        }

    }

    public static FileDoesNotExistMatcher fileDoesNotExist() {
        return new FileDoesNotExistMatcher();
    }

}
