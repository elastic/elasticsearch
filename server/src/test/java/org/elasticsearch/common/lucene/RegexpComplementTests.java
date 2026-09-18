/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.test.ESTestCase;

public class RegexpComplementTests extends ESTestCase {

    public void testPlainRegexpUnchanged() {
        CharacterRunAutomaton run = run("foo.*");
        assertTrue(run.run("foobar"));
        assertFalse(run.run("bar"));
    }

    public void testComplementOfLiteral() {
        CharacterRunAutomaton run = run("~(notStr)");
        assertFalse(run.run("notStr"));
        assertTrue(run.run("other"));
        assertTrue(run.run("notStrX"));
    }

    public void testIntersectionWithComplement() {
        CharacterRunAutomaton run = run("metrics-.*&~(metrics-endpoint\\.metadata_current_default.*)");
        assertTrue(run.run("metrics-apm"));
        assertTrue(run.run("metrics-cpu"));
        assertFalse(run.run("metrics-endpoint.metadata_current_default"));
        assertFalse(run.run("metrics-endpoint.metadata_current_default-9.0.0"));
        assertFalse(run.run("logs-foo"));
    }

    public void testAnyStringMinusPrefixes() {
        CharacterRunAutomaton run = run("@&~(\\.security.*)&~(\\.async-search.*)");
        assertTrue(run.run("logs-1"));
        assertTrue(run.run("foo"));
        assertFalse(run.run(".security"));
        assertFalse(run.run(".security-7"));
        assertFalse(run.run(".async-search"));
        assertFalse(run.run(".async-search-results"));
    }

    public void testHiddenAndIlmHistoryExclusion() {
        CharacterRunAutomaton run = run("~(([.]|ilm-history-).*)");
        assertTrue(run.run("logs-1"));
        assertTrue(run.run("abcde"));
        assertFalse(run.run(".hidden"));
        assertFalse(run.run("ilm-history-3"));
    }

    public void testConcatenatedComplement() {
        CharacterRunAutomaton run = run("\\.fleet-actions~(-results.*)");
        assertTrue(run.run(".fleet-actions-7"));
        assertTrue(run.run(".fleet-actions"));
        assertFalse(run.run(".fleet-actions-results"));
        assertFalse(run.run(".fleet-actions-results-1"));
    }

    public void testTildeInCharacterClassIsLiteral() {
        CharacterRunAutomaton run = run("[a~]b");
        assertTrue(run.run("ab"));
        assertTrue(run.run("~b"));
        assertFalse(run.run("cb"));
    }

    public void testUnbalancedComplement() {
        expectThrows(
            IllegalArgumentException.class,
            () -> RegexpComplement.toAutomaton("~(abc", Operations.DEFAULT_DETERMINIZE_WORK_LIMIT)
        );
    }

    private static CharacterRunAutomaton run(String regex) {
        return new CharacterRunAutomaton(
            Operations.determinize(
                RegexpComplement.toAutomaton(regex, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
            )
        );
    }
}
