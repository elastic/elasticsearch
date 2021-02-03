/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        CuckooFilter filter = new CuckooFilter(randomIntBetween(10000, 100000),
            ((float)randomIntBetween(1, 20)) / 100.0, Randomness.get());

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
        int attempts = 0;
        while (newInstance.getCount() == instance.getCount() && attempts < 100) {
            newInstance.add(hash(randomLong()));
            attempts += 1;
        }
        if (newInstance.equals(instance)) {
            fail("Unable to mutate filter enough to generate a different version. " +
                "Are capacity/precision defaults too low?");
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
