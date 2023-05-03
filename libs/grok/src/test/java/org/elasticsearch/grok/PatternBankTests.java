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
import java.util.Map;
import java.util.TreeMap;

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
        assertEquals("circular reference in pattern [NAME][!!!%{NAME}!!!]", e.getMessage());
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
        assertEquals("circular reference in pattern [NAME][!!!%{NAME}!!!]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> PatternBank.forbidCircularReferences(Map.of("NAME", "!!!%{NAME:name}!!!")));
        assertEquals("circular reference in pattern [NAME][!!!%{NAME:name}!!!]", e.getMessage());

        e = expectThrows(
            IllegalArgumentException.class,
            () -> { PatternBank.forbidCircularReferences(Map.of("NAME", "!!!%{NAME:name:int}!!!")); }
        );
        assertEquals("circular reference in pattern [NAME][!!!%{NAME:name:int}!!!]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new TreeMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME1}!!!");
            PatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference in pattern [NAME2][!!!%{NAME1}!!!] back to pattern [NAME1]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new TreeMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME3}!!!");
            bank.put("NAME3", "!!!%{NAME1}!!!");
            PatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference in pattern [NAME3][!!!%{NAME1}!!!] back to pattern [NAME1] via patterns [NAME2]", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new TreeMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME3}!!!");
            bank.put("NAME3", "!!!%{NAME4}!!!");
            bank.put("NAME4", "!!!%{NAME5}!!!");
            bank.put("NAME5", "!!!%{NAME1}!!!");
            PatternBank.forbidCircularReferences(bank);
        });
        assertEquals(
            "circular reference in pattern [NAME5][!!!%{NAME1}!!!] back to pattern [NAME1] via patterns [NAME2=>NAME3=>NAME4]",
            e.getMessage()
        );
    }

    public void testCircularSelfReference() {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> PatternBank.forbidCircularReferences(Map.of("ANOTHER", "%{INT}", "INT", "%{INT}"))
        );
        assertEquals("circular reference in pattern [INT][%{INT}]", e.getMessage());
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
}
