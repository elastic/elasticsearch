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
package org.elasticsearch.common.util;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CuckooFilterTests extends AbstractWireSerializingTestCase<CuckooFilter> {

    @Override
    protected CuckooFilter createTestInstance() {
        CuckooFilter filter = new CuckooFilter(randomIntBetween(1, 100000),
            ((float)randomIntBetween(1, 50)) / 100.0, Randomness.get());

        int num = randomIntBetween(0, 10);
        for (int i = 0; i < num; i++) {
            filter.add(hash(randomLong()));
        }

        return filter;
    }

    @Override
    protected Writeable.Reader<CuckooFilter> instanceReader() {
        return in -> new CuckooFilter(in, Randomness.get());
    }

    @Override
    protected CuckooFilter mutateInstance(CuckooFilter instance) {
        CuckooFilter newInstance = new CuckooFilter(instance);
        int num = randomIntBetween(1, 10);
        for (int i = 0; i < num; i++) {
            newInstance.add(hash(randomLong()));
        }
        return newInstance;
    }

    public void testExact() {
        CuckooFilter filter = new CuckooFilter(10000, 0.03, Randomness.get());

        for (int i = 0; i < 100; i++) {
            filter.add(hash(i));
        }

        // Was sized sufficiently large that all of these values should be retained
        for (int i = 0; i < 100; i++) {
            assertThat(filter.mightContain(hash(i)), equalTo(true));
        }
    }

    public void testSaturate() {
        CuckooFilter filter = new CuckooFilter(10, 0.03, Randomness.get());
        int counter = 0;
        boolean saturated = false;
        for (int i = 0; i < 100; i++) {
            logger.info("Value: " + i);
            if (filter.add(hash(i)) == false) {
                saturated = true;
            }
            counter += 1;
            if (saturated) {
                break;
            }
        }
        // Unclear when it will saturate exactly, but should be before 100 given the configuration
        assertTrue(saturated);
        logger.info("Saturated at: " + counter);

        for (int i = 0; i < counter; i++) {
            logger.info("Value: " + i);
            assertThat(filter.mightContain(hash(i)), equalTo(true));
        }
    }

    public void testHash() {
        CuckooFilter.hashToIndex(-10, 32);
    }

    public void testBig() {
        CuckooFilter filter = new CuckooFilter(1000000, 0.001, Randomness.get());

        for (int i = 0; i < 10000; i++) {
            filter.add(hash(i));
        }

        int correct = 0;
        int incorrect = 0;
        for (int i = 0; i < 10000; i++) {
            if (filter.mightContain(hash(i))) {
                correct += 1;
            } else {
                incorrect += 1;
            }
        }

        assertThat(correct, equalTo(10000));
        assertThat(incorrect, equalTo(0));

        for (int i = 10000; i < 100000; i++) {
            if (filter.mightContain(hash(i))) {
                incorrect += 1;
            } else {
                correct += 1;
            }
        }

        double fppRate = (double) incorrect / 100000;
        assertThat(fppRate, lessThanOrEqualTo(0.001));
    }

    private long hash(long i) {
        return MurmurHash3.murmur64(i);
    }
}
