/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.junit.BeforeClass;

/** Native SEQUENCE fixtures require the development command to be available in the running build. */
public abstract class AbstractSequenceIntegTestCase extends AbstractEsqlIntegTestCase {
    /** Skip before native-query fixtures run when the command is unavailable. */
    @BeforeClass
    public static void requireSequenceCommand() {
        assumeTrue("requires native SEQUENCE", EsqlCapabilities.Cap.SEQUENCE_COMMAND.isEnabled());
    }
}
