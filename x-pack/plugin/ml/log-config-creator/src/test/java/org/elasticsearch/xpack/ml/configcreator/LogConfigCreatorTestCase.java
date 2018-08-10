/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;

public abstract class LogConfigCreatorTestCase extends LogFileStructureTestCase {

    protected static final Terminal TEST_TERMINAL;
    static {
        TEST_TERMINAL = Terminal.DEFAULT;
        TEST_TERMINAL.setVerbosity(Verbosity.VERBOSE);
    }
}
