/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

public class ProgressInputStreamTests extends ESTestCase {

    private List<Integer> progresses = new ArrayList<>();

    public void testThatProgressListenerIsCalled() throws Exception {
        ProgressInputStream is = newProgressInputStream(0);
        is.checkProgress(-1);

        assertThat(progresses, hasSize(1));
        assertThat(progresses, hasItems(100));
    }

    public void testThatProgressListenerIsCalledOnUnexpectedCompletion() throws Exception {
        ProgressInputStream is = newProgressInputStream(2);
        is.checkProgress(-1);
        assertThat(progresses, hasItems(100));
    }

    public void testThatProgressListenerReturnsMaxValueOnWrongExpectedSize() throws Exception {
        ProgressInputStream is = newProgressInputStream(2);

        is.checkProgress(1);
        assertThat(progresses, hasItems(50));

        is.checkProgress(3);
        assertThat(progresses, hasItems(50, 99));

        is.checkProgress(-1);
        assertThat(progresses, hasItems(50, 99, 100));
    }

    public void testOneByte() throws Exception {
        ProgressInputStream is = newProgressInputStream(1);
        is.checkProgress(1);
        is.checkProgress(-1);

        assertThat(progresses, hasItems(99, 100));

    }

    public void testOddBytes() throws Exception {
        int odd = randomIntBetween(10, 100) * 2 + 1;
        ProgressInputStream is = newProgressInputStream(odd);
        for (int i = 0; i < odd; i++) {
            is.checkProgress(1);
        }
        is.checkProgress(-1);

        assertThat(progresses, hasSize(Math.min(odd + 1, 100)));
        assertThat(progresses, hasItem(100));
    }

    public void testEvenBytes() throws Exception {
        int even = randomIntBetween(10, 100) * 2;
        ProgressInputStream is = newProgressInputStream(even);

        for (int i = 0; i < even; i++) {
            is.checkProgress(1);
        }
        is.checkProgress(-1);

        assertThat(progresses, hasSize(Math.min(even + 1, 100)));
        assertThat(progresses, hasItem(100));
    }

    public void testOnProgressCannotBeCalledMoreThanOncePerPercent() throws Exception {
        int count = randomIntBetween(150, 300);
        ProgressInputStream is = newProgressInputStream(count);

        for (int i = 0; i < count; i++) {
            is.checkProgress(1);
        }
        is.checkProgress(-1);

        assertThat(progresses, hasSize(100));
    }

    private ProgressInputStream newProgressInputStream(int expectedSize) {
        return new ProgressInputStream(null, expectedSize) {
            @Override
            public void onProgress(int percent) {
                progresses.add(percent);
            }
        };
    }
}
