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

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
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
        PainlessExecuteAction.Request request = new PainlessExecuteAction.Request(script, context != null ? context.name : null);
        if (randomBoolean()) {
            request.setIndex(randomAlphaOfLength(4));
        }
        if (randomBoolean()) {
            // TODO: Pass down XContentType here.
            // Otherwise the document's xcontent type is incompatible with the that of the XContentParser xcontent type
//            request.setDocument(new BytesArray("{}"));
//            request.setXContentType(XContentType.JSON);
        }
        if (randomBoolean()) {
            request.setQuery(new MatchAllQueryBuilder());
        }
        return request;
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
        PainlessExecuteAction.Request request = new PainlessExecuteAction.Request(script, (String) null);
        Exception e = request.validate();
        assertNotNull(e);
        assertEquals("Validation Failed: 1: only inline scripts are supported;", e.getMessage());
    }
}
