/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.grok;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomBoolean;

public class PatternBankTests extends ESTestCase {

    public void testInternalBankIsUnmodifiableAndACopy() {
        Map<String, String> bank = new HashMap<>();
        bank.put("ONE", "1");
        var patternBank = new PatternBank(bank);
        assertNotSame(patternBank.bank(), bank);
        assertEquals(patternBank.bank(), bank);
        expectThrows(UnsupportedOperationException.class, () -> { patternBank.bank().put("some", "thing"); });
    }

    public void testBankCannotBeNull() {
        var e = expectThrows(NullPointerException.class, () -> new PatternBank(null));
        assertEquals("bank must not be null", e.getMessage());
    }

    public void testConstructorValidatesCircularReferences() {
        var e = expectThrows(IllegalArgumentException.class, () -> new PatternBank(Map.of("NAME", "!!!%{NAME}!!!")));
        assertEquals("circular reference detected: NAME->NAME", e.getMessage());
    }

    public void testExtendWith() {
        var baseBank = new PatternBank(Map.of("ONE", "1", "TWO", "2"));

        assertSame(baseBank.extendWith(null), baseBank);
        assertSame(baseBank.extendWith(Map.of()), baseBank);

        var extended = baseBank.extendWith(Map.of("THREE", "3", "FOUR", "4"));
        assertNotSame(extended, baseBank);
        assertEquals(extended.bank(), Map.of("ONE", "1", "TWO", "2", "THREE", "3", "FOUR", "4"));
    }

    public void testCircularReference() {
        var e = expectThrows(IllegalArgumentException.class, () -> PatternBank.forbidCircularReferences(Map.of("NAME", "!!!%{NAME}!!!")));
        assertEquals("circular reference detected: NAME->NAME", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> PatternBank.forbidCircularReferences(Map.of("NAME", "!!!%{NAME:name}!!!")));
        assertEquals("circular reference detected: NAME->NAME", e.getMessage());

        e = expectThrows(
            IllegalArgumentException.class,
            () -> { PatternBank.forbidCircularReferences(Map.of("NAME", "!!!%{NAME:name:int}!!!")); }
        );
        assertEquals("circular reference detected: NAME->NAME", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new LinkedHashMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME1}!!!");
            PatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference detected: NAME1->NAME2->NAME1", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new LinkedHashMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME3}!!!");
            bank.put("NAME3", "!!!%{NAME1}!!!");
            PatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference detected: NAME1->NAME2->NAME3->NAME1", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new LinkedHashMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME3}!!!");
            bank.put("NAME3", "!!!%{NAME4}!!!");
            bank.put("NAME4", "!!!%{NAME5}!!!");
            bank.put("NAME5", "!!!%{NAME1}!!!");
            PatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference detected: NAME1->NAME2->NAME3->NAME4->NAME5->NAME1", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new LinkedHashMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME3}!!!");
            bank.put("NAME3", "!!!%{NAME2}!!!");
            PatternBank.forbidCircularReferences(bank);
        });

        assertEquals("circular reference detected: NAME2->NAME3->NAME2", e.getMessage());
        {
            Map<String, String> bank = new HashMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!%{NAME3}%{NAME4}");
            bank.put("NAME2", "!!!%{NAME3}!!!");
            bank.put("NAME3", "!!!!!!");
            bank.put("NAME4", "!!!%{NAME5}!!!");
            bank.put("NAME5", "!!!!!!");
            PatternBank.forbidCircularReferences(bank);
        }

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new LinkedHashMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!%{NAME3}%{NAME4}");
            bank.put("NAME2", "!!!%{NAME3}!!!");
            bank.put("NAME3", "!!!!!!");
            bank.put("NAME4", "!!!%{NAME5}!!!");
            bank.put("NAME5", "!!!%{NAME1}!!!");
            PatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference detected: NAME1->NAME4->NAME5->NAME1", e.getMessage());

        {
            Map<String, String> bank = new HashMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME3}!!!");
            bank.put("NAME3", "!!!!!!");
            bank.put("NAME4", "!!!%{NAME5}!!!");
            bank.put("NAME5", "!!!%{NAME1}!!!");
            PatternBank.forbidCircularReferences(bank);
        }
    }

    public void testCircularSelfReference() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> PatternBank.forbidCircularReferences(Map.of("ANOTHER", "%{INT}", "INT", "%{INT}"))
        );
        assertEquals("circular reference detected: INT->INT", e.getMessage());
    }

    public void testInvalidPatternReferences() {
        var e = expectThrows(IllegalArgumentException.class, () -> PatternBank.forbidCircularReferences(Map.of("NAME", "%{NON_EXISTENT}")));
        assertEquals("pattern [NAME] is referencing a non-existent pattern [NON_EXISTENT]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> PatternBank.forbidCircularReferences(Map.of("NAME", "%{NON_EXISTENT:id}")));
        assertEquals("pattern [NAME] is referencing a non-existent pattern [NON_EXISTENT]", e.getMessage());

        e = expectThrows(
            IllegalArgumentException.class,
            () -> PatternBank.forbidCircularReferences(Map.of("VALID", "valid", "NAME", "%{VALID"))
        );
        assertEquals("pattern [%{VALID] has an invalid syntax", e.getMessage());
    }

    public void testDeepGraphOfPatterns() {
        Map<String, String> patternBankMap = randomBoolean() ? new HashMap<>() : new LinkedHashMap<>();
        final int nodeCount = 20000;
        for (int i = 0; i < nodeCount - 1; i++) {
            patternBankMap.put("FOO" + i, "%{FOO" + (i + 1) + "}");
        }
        patternBankMap.put("FOO" + (nodeCount - 1), "foo");
        new PatternBank(patternBankMap);
    }
}
