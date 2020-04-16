/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
