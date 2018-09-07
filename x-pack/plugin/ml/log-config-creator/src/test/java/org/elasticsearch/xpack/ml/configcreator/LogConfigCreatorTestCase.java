/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.xpack.ml.filestructurefinder.FileStructureTestCase;

import java.util.Arrays;
import java.util.List;

public abstract class LogConfigCreatorTestCase extends FileStructureTestCase {

    protected static final List<String> REDUCED_CHARSETS = Arrays.asList("UTF-8", "UTF-16BE", "UTF-16LE");
    protected static final List<String> POSSIBLE_TIMEZONES = Arrays.asList(null, "Europe/London", "UTC", "+05:30", "-08:00", "EST");
    protected static final List<String> POSSIBLE_HOSTNAMES = Arrays.asList("localhost", "192.168.1.2", "::1", "server", "server.acme.com");

    protected static final String TEST_FILE_NAME = "a_test_file";
    protected static final String TEST_INDEX_NAME = "test";
    protected static final Terminal TEST_TERMINAL;
    static {
        TEST_TERMINAL = Terminal.DEFAULT;
        TEST_TERMINAL.setVerbosity(Verbosity.VERBOSE);
    }

    protected String randomByteOrderMarker(String charset) {
        return Boolean.TRUE.equals(randomHasByteOrderMarker(charset)) ? "\uFEFF" : "";
    }
}
