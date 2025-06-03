/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.retention;

import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockWritableIndexExpander {
    public static WritableIndexExpander create(boolean stateIndexWritable) {
        WritableIndexExpander.initialize(mock(WritableIndexExpander.class));
        WritableIndexExpander writableIndexExpander = WritableIndexExpander.getInstance();
        if (stateIndexWritable) {
            mockWhenIndicesAreWritable(writableIndexExpander);
        } else {
            mockWhenIndicesAreNotWritable(writableIndexExpander);
        }
        return writableIndexExpander;
    }

    private static void mockWhenIndicesAreNotWritable(WritableIndexExpander writableIndexExpander) {
        when(writableIndexExpander.getWritableIndices(anyString()))
            .thenReturn(new ArrayList<>());
        when(writableIndexExpander.getWritableIndices(ArgumentMatchers.<Collection<String>>any()))
            .thenReturn(new ArrayList<>());
    }

    private static void mockWhenIndicesAreWritable(WritableIndexExpander writableIndexExpander) {
        when(writableIndexExpander.getWritableIndices(anyString())).thenAnswer(invocation -> {
            String input = invocation.getArgument(0);
            return new ArrayList<>(List.of(input));
        });
        when(writableIndexExpander.getWritableIndices(ArgumentMatchers.<Collection<String>>any())).thenAnswer(
            invocation -> new ArrayList<>(invocation.getArgument(0))
        );
    }
}
