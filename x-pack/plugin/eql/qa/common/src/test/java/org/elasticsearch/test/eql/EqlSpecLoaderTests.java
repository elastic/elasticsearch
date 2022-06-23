/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class EqlSpecLoaderTests extends ESTestCase {

    public void testLoad() throws Exception {
        List<EqlSpec> specs = EqlSpecLoader.load("/test_spec.toml");
        assertEquals(2, specs.size());

        {
            EqlSpec spec = specs.get(0);
            assertEquals("firstQuery", spec.name());
            assertEquals("process where serial_event_id == 1", spec.query());
            assertNull(spec.note());
            assertNull(spec.description());
            assertNull(spec.tags());
            assertEquals(0, spec.joinKeys().length);
            assertEquals(1, spec.expectedEventIds().size());
            assertArrayEquals(new long[] { 1 }, spec.expectedEventIds().get(0));
        }
        {
            EqlSpec spec = specs.get(1);
            assertEquals("secondQuery", spec.name());
            assertEquals(
                "sample by bool, host\n"
                    + "    [any where uptime > 0 or bool == false]\n"
                    + "    [any where port > 10000 or bool == false]",
                spec.query()
            );
            assertEquals("this is a note", spec.note());
            assertEquals("this is the test description", spec.description());
            assertArrayEquals(new String[] { "foo", "bar" }, spec.tags());
            assertEquals(2, spec.expectedEventIds().size());
            assertArrayEquals(new long[] { 1, 2, 3 }, spec.expectedEventIds().get(0));
            assertArrayEquals(new long[] { 4, 5, 6 }, spec.expectedEventIds().get(1));
            assertEquals(2, spec.joinKeys().length);
            assertArrayEquals(new String[] { "bar", "baz" }, spec.joinKeys());
        }
    }
}
