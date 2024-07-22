/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.arbitrary;

import org.elasticsearch.logsdb.datageneration.FieldType;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLong;

public class RandomBasedArbitrary implements Arbitrary {
    @Override
    public boolean generateSubObject() {
        // Using a static 10% change, this is just a chosen value that can be tweaked.
        return randomDouble() <= 0.1;
    }

    @Override
    public boolean generateNestedObject() {
        // Using a static 1% change, this is just a chosen value that can be tweaked.
        return randomDouble() <= 0.01;
    }

    @Override
    public int childFieldCount(int lowerBound, int upperBound) {
        return randomIntBetween(lowerBound, upperBound);
    }

    @Override
    public String fieldName(int lengthLowerBound, int lengthUpperBound) {
        return randomAlphaOfLengthBetween(lengthLowerBound, lengthUpperBound);
    }

    @Override
    public FieldType fieldType() {
        return randomFrom(FieldType.values());
    }

    @Override
    public long longValue() {
        return randomLong();
    }

    @Override
    public String stringValue(int lengthLowerBound, int lengthUpperBound) {
        return randomAlphaOfLengthBetween(lengthLowerBound, lengthLowerBound);
    }
}
