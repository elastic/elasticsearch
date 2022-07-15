/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.io.ansi.AnsiConsoleLoader;
import org.elasticsearch.test.ESTestCase;

import java.util.function.Supplier;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class ConsoleLoaderTests extends ESTestCase {

    public void testBuildSupplier() {
        final Supplier<ConsoleLoader.Console> supplier = ConsoleLoader.buildConsoleLoader(AnsiConsoleLoader.class.getClassLoader());
        assertThat(supplier, notNullValue());
        assertThat(supplier, instanceOf(AnsiConsoleLoader.class));
    }
}
