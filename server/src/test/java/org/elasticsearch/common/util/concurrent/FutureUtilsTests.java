/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.Future;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FutureUtilsTests extends ESTestCase {

    public void testCancellingNullFutureOkay() {
        FutureUtils.cancel(null);
    }

    public void testRunningFutureNotInterrupted() {
        final Future<?> future = mock(Future.class);
        FutureUtils.cancel(future);
        verify(future).cancel(false);
    }

}
