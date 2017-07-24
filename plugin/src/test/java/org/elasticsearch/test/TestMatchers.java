/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

public class TestMatchers extends Matchers {

    public static Matcher<Path> pathExists(Path path, LinkOption... options) {
        return new CustomMatcher<Path>("Path " + path + " doesn't exist") {
            @Override
            public boolean matches(Object item) {
                return Files.exists(path, options);
            }
        };
    }
}
