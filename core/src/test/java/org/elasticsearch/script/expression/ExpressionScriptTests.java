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
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collections;

public class ExpressionScriptTests extends ESSingleNodeTestCase {

    public void testNeedsScores() {
        IndexService index = createIndex("test", Settings.EMPTY, "type", "d", "type=double");
        
        ExpressionScriptEngineService service = new ExpressionScriptEngineService(Settings.EMPTY);
        SearchLookup lookup = new SearchLookup(index.mapperService(), index.fieldData(), null);

        Object compiled = service.compile("1.2");
        SearchScript ss = service.search(new CompiledScript(ScriptType.INLINE, "randomName", "expression", compiled), lookup, Collections.<String, Object>emptyMap());
        assertFalse(ss.needsScores());

        compiled = service.compile("doc['d'].value");
        ss = service.search(new CompiledScript(ScriptType.INLINE, "randomName", "expression", compiled), lookup, Collections.<String, Object>emptyMap());
        assertFalse(ss.needsScores());

        compiled = service.compile("1/_score");
        ss = service.search(new CompiledScript(ScriptType.INLINE, "randomName", "expression", compiled), lookup, Collections.<String, Object>emptyMap());
        assertTrue(ss.needsScores());

        compiled = service.compile("doc['d'].value * _score");
        ss = service.search(new CompiledScript(ScriptType.INLINE, "randomName", "expression", compiled), lookup, Collections.<String, Object>emptyMap());
        assertTrue(ss.needsScores());
    }

}
