/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static java.util.Map.entry;

public class UtilityTests extends ESTestCase {

    public static final Map<Integer, String> ORDINALS = Map.ofEntries(
        entry(0, "0th"),
        entry(1, "1st"),
        entry(2, "2nd"),
        entry(3, "3rd"),
        entry(4, "4th"),
        entry(5, "5th"),
        entry(10, "10th"),
        entry(11, "11th"),
        entry(12, "12th"),
        entry(13, "13th"),
        entry(14, "14th"),
        entry(20, "20th"),
        entry(21, "21st"),
        entry(22, "22nd"),
        entry(23, "23rd"),
        entry(24, "24th"),
        entry(100, "100th"),
        entry(101, "101st"),
        entry(102, "102nd"),
        entry(103, "103rd"),
        entry(104, "104th"),
        entry(111, "111th"),
        entry(112, "112th"),
        entry(113, "113th"),
        entry(114, "114th"),
        entry(1000, "1000th")
    );

    public void testToOrdinal() {
        for (Map.Entry<Integer, String> item : ORDINALS.entrySet()) {
            assertEquals(Utility.toOrdinal(item.getKey()), item.getValue());
        }
    }
}
