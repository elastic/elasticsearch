/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.grok;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.TreeMap;

public class GrokPatternBankTests extends ESTestCase {

    public void testCircularReference() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = Map.of("NAME", "!!!%{NAME}!!!");
            GrokPatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference in pattern [NAME][!!!%{NAME}!!!]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            var bank = Map.of("NAME", "!!!%{NAME:name}!!!");
            GrokPatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference in pattern [NAME][!!!%{NAME:name}!!!]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            var bank = Map.of("NAME", "!!!%{NAME:name:int}!!!");
            GrokPatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference in pattern [NAME][!!!%{NAME:name:int}!!!]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new TreeMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME1}!!!");
            GrokPatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference in pattern [NAME2][!!!%{NAME1}!!!] back to pattern [NAME1]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new TreeMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME3}!!!");
            bank.put("NAME3", "!!!%{NAME1}!!!");
            GrokPatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference in pattern [NAME3][!!!%{NAME1}!!!] back to pattern [NAME1] via patterns [NAME2]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new TreeMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME3}!!!");
            bank.put("NAME3", "!!!%{NAME4}!!!");
            bank.put("NAME4", "!!!%{NAME5}!!!");
            bank.put("NAME5", "!!!%{NAME1}!!!");
            GrokPatternBank.forbidCircularReferences(bank);
        });
        assertEquals(
            "circular reference in pattern [NAME5][!!!%{NAME1}!!!] back to pattern [NAME1] via patterns [NAME2=>NAME3=>NAME4]",
            e.getMessage()
        );
    }

    public void testCircularSelfReference() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> {
            var bank = Map.of("ANOTHER", "%{INT}", "INT", "%{INT}");
            GrokPatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference in pattern [INT][%{INT}]", e.getMessage());
    }
}
