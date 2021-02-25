/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Test that needsScores() is reported correctly depending on whether _score is used
 */
// TODO: can we test this better? this is a port of the ExpressionsTests method.
public class NeedsScoreTests extends ESSingleNodeTestCase {

    public void testNeedsScores() {
        IndexService index = createIndex("test", Settings.EMPTY, "type", "d", "type=double");

        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        contexts.put(NumberSortScript.CONTEXT, Whitelist.BASE_WHITELISTS);
        PainlessScriptEngine service = new PainlessScriptEngine(Settings.EMPTY, contexts);

        SearchExecutionContext searchExecutionContext = index.newSearchExecutionContext(0, 0, null, () -> 0, null, emptyMap());

        NumberSortScript.Factory factory = service.compile(null, "1.2", NumberSortScript.CONTEXT, Collections.emptyMap());
        NumberSortScript.LeafFactory ss = factory.newFactory(Collections.emptyMap(), searchExecutionContext.lookup());
        assertFalse(ss.needs_score());

        factory = service.compile(null, "doc['d'].value", NumberSortScript.CONTEXT, Collections.emptyMap());
        ss = factory.newFactory(Collections.emptyMap(), searchExecutionContext.lookup());
        assertFalse(ss.needs_score());

        factory = service.compile(null, "1/_score", NumberSortScript.CONTEXT, Collections.emptyMap());
        ss = factory.newFactory(Collections.emptyMap(), searchExecutionContext.lookup());
        assertTrue(ss.needs_score());

        factory = service.compile(null, "doc['d'].value * _score", NumberSortScript.CONTEXT, Collections.emptyMap());
        ss = factory.newFactory(Collections.emptyMap(), searchExecutionContext.lookup());
        assertTrue(ss.needs_score());
    }
}
