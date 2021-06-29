/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.elasticsearch.test.ESTestCase;

import java.io.IOError;
import java.io.UncheckedIOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class WriteStateExceptionTests extends ESTestCase {

    public void testDirtyFlag() {
        boolean dirty = randomBoolean();
        WriteStateException ex = new WriteStateException(dirty, "test", null);
        assertThat(ex.isDirty(), equalTo(dirty));
    }

    public void testNonDirtyRethrow() {
        WriteStateException ex = new WriteStateException(false, "test", null);
        UncheckedIOException ex2 = expectThrows(UncheckedIOException.class, () -> ex.rethrowAsErrorOrUncheckedException());
        assertThat(ex2.getCause(), instanceOf(WriteStateException.class));
    }

    public void testDirtyRethrow() {
        WriteStateException ex = new WriteStateException(true, "test", null);
        IOError err = expectThrows(IOError.class, () -> ex.rethrowAsErrorOrUncheckedException());
        assertThat(err.getCause(), instanceOf(WriteStateException.class));
    }
}
