/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.logsdb.datageneration.arbitrary.RandomBasedArbitrary;
import org.elasticsearch.logsdb.datageneration.fields.FieldValues;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.function.Supplier;

public class FieldValuesTest extends ESTestCase {
    public void testSanity() {
        Supplier<Object> values = () -> 100;
        var arbitrary = new RandomBasedArbitrary();

        var valuesWithNullsAndWrappedInArray = FieldValues.injectNulls(arbitrary).andThen(FieldValues.wrappedInArray(arbitrary)).apply(values);
        var value = valuesWithNullsAndWrappedInArray.get();

        if (value instanceof List<?> list) {
            assertTrue(list.size() <= 5);
            for (var item : list) {
                if (item instanceof Integer intValue) {
                    assertEquals(Integer.valueOf(100), intValue);
                } else {
                    assertNull(item);
                }
            }
        } else if (value instanceof Integer intValue) {
            assertEquals(Integer.valueOf(100), intValue);
        } else {
            assertNull(value);
        }
    }
}
