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
package org.elasticsearch.painless;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.painless.PainlessExecuteAction.Request.ContextSetup;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.Collections;

public class PainlessExecuteRequestTests extends AbstractStreamableXContentTestCase<PainlessExecuteAction.Request> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedXContents());
    }

    @Override
    protected PainlessExecuteAction.Request createTestInstance() {
        Script script = new Script(randomAlphaOfLength(10));
        ScriptContext<?> context = randomBoolean() ? randomFrom(PainlessExecuteAction.Request.SUPPORTED_CONTEXTS.values()) : null;
        ContextSetup contextSetup = randomBoolean() ? randomContextSetup() : null;
        return new PainlessExecuteAction.Request(script, context != null ? context.name : null, contextSetup);
    }

    @Override
    protected PainlessExecuteAction.Request createBlankInstance() {
        return new PainlessExecuteAction.Request();
    }

    @Override
    protected PainlessExecuteAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PainlessExecuteAction.Request.parse(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testValidate() {
        Script script = new Script(ScriptType.STORED, null, randomAlphaOfLength(10), Collections.emptyMap());
        PainlessExecuteAction.Request request = new PainlessExecuteAction.Request(script, null, null);
        Exception e = request.validate();
        assertNotNull(e);
        assertEquals("Validation Failed: 1: only inline scripts are supported;", e.getMessage());
    }

    private static ContextSetup randomContextSetup() {
        String index = randomBoolean() ? randomAlphaOfLength(4) : null;
        QueryBuilder query = randomBoolean() ? new MatchAllQueryBuilder() : null;
        // TODO: pass down XContextType to createTestInstance() method.
        // otherwise the document itself is different causing test failures.
        // This should be done in a seperate change as the test instance is created before xcontent type is randomly picked and
        // all the createTestInstance() methods need to be changed, which will make this a big chnage
//        BytesReference doc = randomBoolean() ? new BytesArray("{}") : null;
        BytesReference doc = null;

        ContextSetup contextSetup = new ContextSetup(index, doc, query);
//        if (doc != null) {
//            contextSetup.setXContentType(XContentType.JSON);
//        }
        return contextSetup;
    }
}
