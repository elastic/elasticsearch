/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.painless.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.painless.action.PainlessExecuteAction.Request.ContextSetup;
import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class PainlessExecuteRequestTests extends AbstractWireSerializingTestCase<PainlessExecuteAction.Request> {

    private static final List<ScriptContext<?>> NON_DEFAULT_CONTEXTS = PainlessExecuteAction.Request.SUPPORTED_CONTEXTS.values()
        .stream()
        .filter(context -> context != PainlessTestScript.CONTEXT)
        .toList();

    // Testing XContent serialization manually here, because the xContentType field in ContextSetup determines
    // how the request needs to parse and the xcontent serialization framework randomizes that. The XContentType
    // is not known and accessible when the test request instance is created in the xcontent serialization framework.
    // Changing that is a big change. Writing a custom xcontent test here is the best option for now, because as far
    // as I know this request class is the only case where this is a problem.
    public final void testFromXContent() throws Exception {
        for (int i = 0; i < 20; i++) {
            PainlessExecuteAction.Request testInstance = createTestInstance();
            ContextSetup contextSetup = testInstance.getContextSetup();
            XContent xContent = randomFrom(XContentType.values()).xContent();
            if (contextSetup != null && contextSetup.getXContentType() != null) {
                xContent = contextSetup.getXContentType().xContent();
            }

            try (XContentBuilder builder = XContentBuilder.builder(xContent)) {
                builder.value(testInstance);
                try (XContentParser parser = createParser(xContent, BytesReference.bytes(builder))) {
                    PainlessExecuteAction.Request result = PainlessExecuteAction.Request.parse(parser);
                    assertThat(result, equalTo(testInstance));
                }
            }
        }
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
    }

    @Override
    protected PainlessExecuteAction.Request createTestInstance() {
        Script script = new Script(randomAlphaOfLength(10));
        ScriptContext<?> context = randomBoolean() ? randomFrom(PainlessExecuteAction.Request.SUPPORTED_CONTEXTS.values()) : null;
        ContextSetup contextSetup = randomBoolean() ? randomContextSetup() : null;
        return new PainlessExecuteAction.Request(script, context != null ? context.name : null, contextSetup);
    }

    @Override
    protected PainlessExecuteAction.Request mutateInstance(PainlessExecuteAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<PainlessExecuteAction.Request> instanceReader() {
        return PainlessExecuteAction.Request::new;
    }

    public void testValidate() {
        Script script = new Script(ScriptType.STORED, null, randomAlphaOfLength(10), Collections.emptyMap());
        PainlessExecuteAction.Request request = new PainlessExecuteAction.Request(script, null, null);
        Exception e = request.validate();
        assertNotNull(e);
        assertEquals("Validation Failed: 1: only inline scripts are supported;", e.getMessage());
    }

    public void testValidateWithoutContextSetup() {
        for (ScriptContext<?> context : NON_DEFAULT_CONTEXTS) {
            ActionRequestValidationException e = requestWithContext(context, null).validate();
            assertNotNull("context [" + context.name + "] without context_setup must fail validation", e);
            assertThat(e.validationErrors(), contains("context_setup is a required parameter for current context"));
        }
    }

    public void testValidateWithoutIndex() {
        for (ScriptContext<?> context : NON_DEFAULT_CONTEXTS) {
            ActionRequestValidationException e = requestWithContext(context, new ContextSetup(null, new BytesArray("{}"), null)).validate();
            assertNotNull("context [" + context.name + "] without index must fail validation", e);
            assertThat(e.validationErrors(), contains("index is a required parameter for current context"));
        }
    }

    public void testValidateWithoutDocument() {
        for (ScriptContext<?> context : NON_DEFAULT_CONTEXTS) {
            ActionRequestValidationException e = requestWithContext(context, new ContextSetup(randomAlphaOfLength(4), null, null))
                .validate();
            assertNotNull("context [" + context.name + "] without document must fail validation", e);
            assertThat(e.validationErrors(), contains("document is a required parameter for current context"));
        }
    }

    public void testValidateWithoutIndexAndDocument() {
        for (ScriptContext<?> context : NON_DEFAULT_CONTEXTS) {
            ActionRequestValidationException e = requestWithContext(context, new ContextSetup(null, null, null)).validate();
            assertNotNull("context [" + context.name + "] without index and document must fail validation", e);
            assertThat(
                e.validationErrors(),
                contains("index is a required parameter for current context", "document is a required parameter for current context")
            );
        }
    }

    public void testValidateWithIndexAndDocument() {
        for (ScriptContext<?> context : NON_DEFAULT_CONTEXTS) {
            ContextSetup contextSetup = new ContextSetup(randomAlphaOfLength(4), new BytesArray("{}"), null);
            assertNull(
                "context [" + context.name + "] with index and document must be valid",
                requestWithContext(context, contextSetup).validate()
            );
        }
    }

    public void testValidateDefaultContextNeedsNoContextSetup() {
        // The default painless_test context needs neither a context_setup, nor an index or a document in it
        assertNull(requestWithContext(null, null).validate());
        assertNull(requestWithContext(PainlessTestScript.CONTEXT, null).validate());
        assertNull(requestWithContext(PainlessTestScript.CONTEXT, new ContextSetup(null, null, null)).validate());
    }

    private static PainlessExecuteAction.Request requestWithContext(ScriptContext<?> context, ContextSetup contextSetup) {
        return new PainlessExecuteAction.Request(new Script(randomAlphaOfLength(10)), context != null ? context.name : null, contextSetup);
    }

    private static ContextSetup randomContextSetup() {
        String index = randomBoolean() ? randomAlphaOfLength(4) : null;
        QueryBuilder query = randomBoolean() ? new MatchAllQueryBuilder() : null;
        BytesReference doc = null;
        XContentType xContentType = randomFrom(XContentType.values()).canonical();
        if (randomBoolean()) {
            try {
                XContentBuilder xContentBuilder = XContentBuilder.builder(xContentType.xContent());
                xContentBuilder.startObject();
                xContentBuilder.endObject();
                doc = BytesReference.bytes(xContentBuilder);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        ContextSetup contextSetup = new ContextSetup(index, doc, query);
        contextSetup.setXContentType(xContentType);
        return contextSetup;
    }
}
