/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration;

import org.elasticsearch.logsdb.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.logsdb.datageneration.datasource.DefaultWrappersHandler;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.function.Supplier;

public class DefaultWrappersHandlerTests extends ESTestCase {
    public void testSanity() {
        var sut = new DefaultWrappersHandler();

        Supplier<Object> values = () -> 100;
        var nulls = sut.handle(new DataSourceRequest.NullWrapper());
        var arrays = sut.handle(new DataSourceRequest.ArrayWrapper());

        var valuesWithNullsAndWrappedInArray = arrays.wrapper().compose(nulls.wrapper()).apply(values);

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
