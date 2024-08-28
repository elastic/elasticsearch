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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.hamcrest.Matchers.containsString;

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

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new LinkedHashMap<>();
            bank.put("NAME1", "!!!%{NAME2}!!!");
            bank.put("NAME2", "!!!%{NAME2}!!%{NAME3}!");
            bank.put("NAME3", "!!!%{NAME1}!!!");
            PatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference detected: NAME1->NAME2->NAME3->NAME1", e.getMessage());

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

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new LinkedHashMap<>();
            bank.put("NAME1", "!!!%{NAME2} %{NAME3}!!!");
            bank.put("NAME2", "!!!%{NAME4} %{NAME5}!!!");
            bank.put("NAME3", "!!!!!!");
            bank.put("NAME4", "!!!!!!");
            bank.put("NAME5", "!!!%{NAME1}!!!");
            PatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference detected: NAME1->NAME2->NAME5->NAME1", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> {
            Map<String, String> bank = new LinkedHashMap<>();
            bank.put("NAME1", "!!!%{NAME2} %{NAME3}!!!");
            bank.put("NAME2", "!!!%{NAME4} %{NAME5}!!!");
            bank.put("NAME3", "!!!%{NAME1}!!!");
            bank.put("NAME4", "!!!!!!");
            bank.put("NAME5", "!!!!!!");
            PatternBank.forbidCircularReferences(bank);
        });
        assertEquals("circular reference detected: NAME1->NAME3->NAME1", e.getMessage());
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
        final int nodeCount = 20_000;
        for (int i = 0; i < nodeCount - 1; i++) {
            patternBankMap.put("FOO" + i, "%{FOO" + (i + 1) + "}");
        }
        patternBankMap.put("FOO" + (nodeCount - 1), "foo");
        new PatternBank(patternBankMap);
    }

    public void testRandomBanksWithoutCycles() {
        /*
         * This creates a large number of pattens, each of which refers to a large number of patterns. But there are no cycles in any of
         * these since each pattern only references patterns with a higher ID. We don't expect any exceptions here.
         */
        Map<String, String> patternBankMap = randomBoolean() ? new HashMap<>() : new LinkedHashMap<>();
        final int nodeCount = 500;
        for (int i = 0; i < nodeCount - 1; i++) {
            StringBuilder patternBuilder = new StringBuilder();
            for (int j = 0; j < randomIntBetween(0, 20); j++) {
                patternBuilder.append("%{FOO-" + randomIntBetween(i + 1, nodeCount - 1) + "}");
            }
            patternBankMap.put("FOO-" + i, patternBuilder.toString());
        }
        patternBankMap.put("FOO-" + (nodeCount - 1), "foo");
        new PatternBank(patternBankMap);
    }

    public void testRandomBanksWithCycles() {
        /*
         * This creates a large number of pattens, each of which refers to a large number of patterns. We have at least one cycle because
         * we pick a node at random, and make sure that a node that it links (or one of its descendants) to links back. If no descendant
         * links back to it, we create an artificial cycle at the end.
         */
        Map<String, String> patternBankMap = new LinkedHashMap<>();
        final int nodeCount = 500;
        int nodeToHaveCycle = randomIntBetween(0, nodeCount);
        int nodeToPotentiallyCreateCycle = -1;
        boolean haveCreatedCycle = false;
        for (int i = 0; i < nodeCount - 1; i++) {
            StringBuilder patternBuilder = new StringBuilder();
            int numberOfLinkedPatterns = randomIntBetween(1, 20);
            int nodeToLinkBackIndex = randomIntBetween(0, numberOfLinkedPatterns);
            Set<Integer> childNodes = new HashSet<>();
            for (int j = 0; j < numberOfLinkedPatterns; j++) {
                int childNode = randomIntBetween(i + 1, nodeCount - 1);
                childNodes.add(childNode);
                patternBuilder.append("%{FOO-" + childNode + "}");
                if (i == nodeToHaveCycle) {
                    if (nodeToLinkBackIndex == j) {
                        nodeToPotentiallyCreateCycle = childNode;
                    }
                }
            }
            if (i == nodeToPotentiallyCreateCycle) {
                // We either create the cycle here, or randomly pick a child node to maybe create the cycle
                if (randomBoolean()) {
                    patternBuilder.append("%{FOO-" + nodeToHaveCycle + "}");
                    haveCreatedCycle = true;
                } else {
                    nodeToPotentiallyCreateCycle = randomFrom(childNodes);
                }
            }
            patternBankMap.put("FOO-" + i, patternBuilder.toString());
        }
        if (haveCreatedCycle) {
            patternBankMap.put("FOO-" + (nodeCount - 1), "foo");
        } else {
            // We didn't randomly create a cycle, so just force one in this last pattern
            nodeToHaveCycle = nodeCount - 1;
            patternBankMap.put("FOO-" + nodeToHaveCycle, "%{FOO-" + nodeToHaveCycle + "}");
        }
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new PatternBank(patternBankMap));
        assertThat(e.getMessage(), containsString("FOO-" + nodeToHaveCycle));
    }
}
