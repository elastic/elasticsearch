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
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.text.ParseException;
import java.util.Collections;

public class ExpressionTests extends ESSingleNodeTestCase {
    ExpressionScriptEngineService service;
    SearchLookup lookup;
    
    @Override
    public void setUp() throws Exception {
        super.setUp();
        IndexService index = createIndex("test", Settings.EMPTY, "type", "d", "type=double");
        service = new ExpressionScriptEngineService(Settings.EMPTY);
        lookup = new SearchLookup(index.mapperService(), index.fieldData(), null);
    }
    
    private SearchScript compile(String expression) {
        Object compiled = service.compile(null, expression, Collections.emptyMap());
        return service.search(new CompiledScript(ScriptType.INLINE, "randomName", "expression", compiled), lookup, Collections.<String, Object>emptyMap());
    }

    public void testNeedsScores() {
        assertFalse(compile("1.2").needsScores());
        assertFalse(compile("doc['d'].value").needsScores());
        assertTrue(compile("1/_score").needsScores());
        assertTrue(compile("doc['d'].value * _score").needsScores());
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
