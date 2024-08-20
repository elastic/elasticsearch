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
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.painless.ScriptTestCase.PAINLESS_BASE_WHITELIST;

public class ScoreScriptTests extends ESSingleNodeTestCase {
    /**
     * Test that needTermStats() is reported correctly depending on whether _termStats is used
     */
    public void testNeedsTermStats() {
        IndexService index = createIndex("test", Settings.EMPTY, "type", "d", "type=double");

        Map<ScriptContext<?>, List<Whitelist>> contexts = new HashMap<>();
        List<Whitelist> whitelists = new ArrayList<>(PAINLESS_BASE_WHITELIST);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(PainlessPlugin.class, "org.elasticsearch.script.score.txt"));
        contexts.put(ScoreScript.CONTEXT, whitelists);
        PainlessScriptEngine service = new PainlessScriptEngine(Settings.EMPTY, contexts);

        SearchExecutionContext searchExecutionContext = index.newSearchExecutionContext(0, 0, null, () -> 0, null, emptyMap());

        ScoreScript.Factory factory = service.compile(null, "1.2", ScoreScript.CONTEXT, Collections.emptyMap());
        ScoreScript.LeafFactory ss = factory.newFactory(Collections.emptyMap(), searchExecutionContext.lookup());
        assertFalse(ss.needs_termStats());

        factory = service.compile(null, "doc['d'].value", ScoreScript.CONTEXT, Collections.emptyMap());
        ss = factory.newFactory(Collections.emptyMap(), searchExecutionContext.lookup());
        assertFalse(ss.needs_termStats());

        factory = service.compile(null, "1/_termStats.totalTermFreq().getAverage()", ScoreScript.CONTEXT, Collections.emptyMap());
        ss = factory.newFactory(Collections.emptyMap(), searchExecutionContext.lookup());
        assertTrue(ss.needs_termStats());

        factory = service.compile(null, "doc['d'].value * _termStats.docFreq().getSum()", ScoreScript.CONTEXT, Collections.emptyMap());
        ss = factory.newFactory(Collections.emptyMap(), searchExecutionContext.lookup());
        assertTrue(ss.needs_termStats());
    }
}
