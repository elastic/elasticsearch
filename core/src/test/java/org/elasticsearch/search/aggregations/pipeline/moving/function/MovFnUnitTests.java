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

package org.elasticsearch.search.aggregations.pipeline.moving.function;

import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MaxModel;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MedianModel;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MinModel;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MovModel;
import org.elasticsearch.search.aggregations.pipeline.moving.models.SumModel;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.test.hamcrest.DoubleMatcher.nearlyEqual;

public class MovFnUnitTests extends ESTestCase {
    public void testMinFn() {
        MovModel model = new MinModel();

        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            double randValue = randomDouble();
            double expected = Double.MAX_VALUE;

            if (i == 0) {
                window.offer(randValue);
                continue;
            }

            for (double value : window) {
                expected = Math.min(expected, value);
            }

            double actual = model.next(window);
            assertTrue("actual does not match expected [" + actual + " vs " + expected + "]",
                nearlyEqual(actual, expected, 0.1));
            window.offer(randValue);
        }
    }

    public void testMaxFn() {
        MovModel model = new MaxModel();

        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            double randValue = randomDouble();
            double expected = -Double.MAX_VALUE;

            if (i == 0) {
                window.offer(randValue);
                continue;
            }

            for (double value : window) {
                expected = Math.max(expected, value);
            }

            double actual = model.next(window);
            assertTrue("actual does not match expected [" + actual + " vs " + expected + "]",
                nearlyEqual(actual, expected, 0.1));
            window.offer(randValue);
        }
    }

    public void testMedianOddFn() {
        MovModel model = new MedianModel();

        int windowSize = 11;

        double[] values = {1,10,3,9,5,6,7,8,4,2,11};
        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (double v : values) {
            window.offer(v);
        }

        double actual = model.next(window);

        assertEquals(6, actual, 0.0001);
    }

    public void testMedianEvenFn() {
        MovModel model = new MedianModel();

        int windowSize = 11;

        double[] values = {1,10,3,9,5,6,7,8,4,2};
        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (double v : values) {
            window.offer(v);
        }

        double actual = model.next(window);

        assertEquals(5.5, actual, 0.0001);
    }

    public void testSumFn() {
        MovModel model = new SumModel();

        int numValues = randomIntBetween(1, 100);
        int windowSize = randomIntBetween(1, 50);

        EvictingQueue<Double> window = new EvictingQueue<>(windowSize);
        for (int i = 0; i < numValues; i++) {

            double randValue = randomDouble();
            double expected = 0;

            if (i == 0) {
                window.offer(randValue);
                continue;
            }

            for (double value : window) {
                expected += value;
            }

            double actual = model.next(window);
            assertTrue("actual does not match expected [" + actual + " vs " + expected + "]",
                nearlyEqual(actual, expected, 0.1));
            window.offer(randValue);
        }
    }
}
