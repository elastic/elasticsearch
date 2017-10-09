/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script.expression;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.text.ParseException;
import java.util.Collections;

public class ExpressionTests extends ESSingleNodeTestCase {
    ExpressionScriptEngine service;
    SearchLookup lookup;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        IndexService index = createIndex("test", Settings.EMPTY, "type", "d", "type=double");
        service = new ExpressionScriptEngine(Settings.EMPTY);
        QueryShardContext shardContext = index.newQueryShardContext(0, null, () -> 0, null);
        lookup = new SearchLookup(index.mapperService(), shardContext::getForField, null);
    }

    private SearchScript.LeafFactory compile(String expression) {
        SearchScript.Factory factory = service.compile(null, expression, SearchScript.CONTEXT, Collections.emptyMap());
        return factory.newFactory(Collections.emptyMap(), lookup);
    }

    public void testNeedsScores() {
        assertFalse(compile("1.2").needs_score());
        assertFalse(compile("doc['d'].value").needs_score());
        assertTrue(compile("1/_score").needs_score());
        assertTrue(compile("doc['d'].value * _score").needs_score());
    }

    public void testCompileError() {
        ScriptException e = expectThrows(ScriptException.class, () -> {
            compile("doc['d'].value * *@#)(@$*@#$ + 4");
        });
        assertTrue(e.getCause() instanceof ParseException);
    }

    public void testLinkError() {
        ScriptException e = expectThrows(ScriptException.class, () -> {
            compile("doc['e'].value * 5");
        });
        assertTrue(e.getCause() instanceof ParseException);
    }
}
