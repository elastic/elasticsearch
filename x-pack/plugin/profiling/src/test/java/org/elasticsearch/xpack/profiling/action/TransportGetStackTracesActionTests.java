/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransportGetStackTracesActionTests extends ESTestCase {
    public void testSliceEmptyList() {
        assertEquals(List.of(List.of()), TransportGetStackTracesAction.sliced(Collections.emptyList(), 4));
    }

    public void testSingleSlice() {
        List<String> input = randomList(2, 5, () -> randomAlphaOfLength(3));
        List<List<String>> sliced = TransportGetStackTracesAction.sliced(input, 1);
        assertEquals(1, sliced.size());
        assertEquals(input, sliced.get(0));
    }

    public void testSliceListSmallerOrEqualToSliceCount() {
        int slices = 7;
        List<String> input = randomList(0, slices, () -> randomAlphaOfLength(3));
        List<List<String>> sliced = TransportGetStackTracesAction.sliced(input, slices);
        assertEquals(1, sliced.size());
        assertEquals(input, sliced.get(0));
    }

    public void testSliceListMultipleOfSliceCount() {
        int slices = 2;
        List<String> input = List.of("a", "b", "c", "d");
        List<List<String>> sliced = TransportGetStackTracesAction.sliced(input, slices);
        assertEquals(slices, sliced.size());
        assertEquals(List.of("a", "b"), sliced.get(0));
        assertEquals(List.of("c", "d"), sliced.get(1));
    }

    public void testSliceListGreaterThanSliceCount() {
        int slices = 3;
        List<String> input = List.of("a", "b", "c", "d", "e", "f", "g", "h", "i", "j");
        List<List<String>> sliced = TransportGetStackTracesAction.sliced(input, slices);
        assertEquals(slices, sliced.size());
        assertEquals(List.of("a", "b", "c"), sliced.get(0));
        assertEquals(List.of("d", "e", "f"), sliced.get(1));
        assertEquals(List.of("g", "h", "i", "j"), sliced.get(2));
    }

    public void testRandomLengthListGreaterThanSliceCount() {
        int slices = randomIntBetween(1, 16);
        // To ensure that we can actually slice the list
        List<String> input = randomList(slices + 1, 20000, () -> "s");
        List<List<String>> sliced = TransportGetStackTracesAction.sliced(input, slices);
        assertEquals(slices, sliced.size());
    }

    public void testDetailsHandlerOnConcurrentFailure() throws InterruptedException {
        ActionListener<GetStackTracesResponse> listener = mock();

        var handler = new TransportGetStackTracesAction.DetailsHandler(mock(), listener, 0, 0, 1, 2);

        var executables1 = new MultiGetItemResponse(null, new MultiGetResponse.Failure("executables", "1", new RuntimeException()));
        var stackframes1 = new MultiGetItemResponse(null, new MultiGetResponse.Failure("stackframes", "1", new RuntimeException()));
        var stackframes2 = new MultiGetItemResponse(null, new MultiGetResponse.Failure("stackframes", "2", new RuntimeException()));

        var t1 = Thread.ofVirtual()
            .start(() -> handler.onExecutableDetailsResponse(new MultiGetResponse(new MultiGetItemResponse[] { executables1 })));
        var t2 = Thread.ofVirtual()
            .start(() -> handler.onExecutableDetailsResponse(new MultiGetResponse(new MultiGetItemResponse[] { stackframes1 })));
        var t3 = Thread.ofVirtual()
            .start(() -> handler.onStackFramesResponse(new MultiGetResponse(new MultiGetItemResponse[] { stackframes2 })));

        t1.join();
        t2.join();
        t3.join();

        verify(listener, times(1)).onFailure(assertArg(failure -> {
            assertThat(
                failure,
                anyOf(
                    is(executables1.getFailure().getFailure()),
                    is(stackframes1.getFailure().getFailure()),
                    is(stackframes2.getFailure().getFailure())
                )
            );
            assertThat(failure.getSuppressed(), arrayWithSize(2));
        }));
    }
}
