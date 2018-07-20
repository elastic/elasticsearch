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

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.coordination.RunnableUtils.labelRunnable;
import static org.hamcrest.core.Is.is;

public class RunnableUtilsTests extends ESTestCase {
    public void testLabelRunnable() {
        final AtomicInteger counter = new AtomicInteger();
        final String expectedMessage = randomAlphaOfLength(10);

        final Runnable labelledRunnable = labelRunnable(counter::incrementAndGet, expectedMessage);

        assertThat(labelledRunnable.toString(), is(expectedMessage));

        assertThat(counter.get(), is(0));
        labelledRunnable.run();
        assertThat(counter.get(), is(1));
        labelledRunnable.run();
        assertThat(counter.get(), is(2));

        assertThat(labelledRunnable.toString(), is(expectedMessage));
    }
}
