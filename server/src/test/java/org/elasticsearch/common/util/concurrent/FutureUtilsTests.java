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