/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

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