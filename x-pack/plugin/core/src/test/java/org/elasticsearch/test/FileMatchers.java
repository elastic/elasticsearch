/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test;

import org.hamcrest.CustomMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

public class FileMatchers {
    public static Matcher<Path> pathExists(LinkOption... options) {
        return new CustomMatcher<>("Path exists") {
            @Override
            public boolean matches(Object item) {
                if (item instanceof Path) {
                    Path path = (Path) item;
                    return Files.exists(path, options);
                } else {
                    return false;
                }

            }
        };
    }

    public static Matcher<Path> isDirectory(LinkOption... options) {
        return new FileTypeMatcher("directory", options) {
            @Override
            protected boolean matchPath(Path path) {
                return Files.isDirectory(path, options);
            }
        };
    }

    public static Matcher<Path> isRegularFile(LinkOption... options) {
        return new FileTypeMatcher("regular file", options) {
            @Override
            protected boolean matchPath(Path path) {
                return Files.isRegularFile(path, options);
            }
        };
    }

    private abstract static class FileTypeMatcher extends CustomMatcher<Path> {
        private final LinkOption[] options;

        FileTypeMatcher(String typeName, LinkOption... options) {
            super("Path is " + typeName);
            this.options = options;
        }

        @Override
        public boolean matches(Object item) {
            if (item instanceof Path) {
                Path path = (Path) item;
                return matchPath(path);
            } else {
                return false;
            }
        }

        protected abstract boolean matchPath(Path path);

        @Override
        public void describeMismatch(Object item, Description description) {
            super.describeMismatch(item, description);
            if (item instanceof Path) {
                Path path = (Path) item;
                if (Files.exists(path, options) == false) {
                    description.appendText(" (file not found)");
                } else if (Files.isDirectory(path, options)) {
                    description.appendText(" (directory)");
                } else if (Files.isSymbolicLink(path)) {
                    description.appendText(" (symlink)");
                } else if (Files.isRegularFile(path, options)) {
                    description.appendText(" (regular file)");
                } else {
                    description.appendText(" (unknown file type)");
                }
            }
        }
    }
}
