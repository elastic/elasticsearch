/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.script.ScriptException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class DebugTests extends ScriptTestCase {
    private final PainlessLookup painlessLookup = PainlessLookupBuilder.buildFromWhitelists(
        PAINLESS_BASE_WHITELIST,
        new HashMap<>(),
        new HashMap<>()
    );

    public void testExplain() {
        // Debug.explain can explain an object
        Object dummy = new Object();
        var wrapper = expectScriptThrows(ErrorCauseWrapper.class, () -> exec("Debug.explain(params.a)", singletonMap("a", dummy), true));
        assertThat(wrapper.realCause.getClass(), equalTo(PainlessExplainError.class));
        var e = (PainlessExplainError) wrapper.realCause;
        assertSame(dummy, e.getObjectToExplain());
        assertThat(e.getHeaders(painlessLookup), hasEntry("es.to_string", singletonList(dummy.toString())));
        assertThat(e.getHeaders(painlessLookup), hasEntry("es.java_class", singletonList("java.lang.Object")));
        assertThat(e.getHeaders(painlessLookup), hasEntry("es.painless_class", singletonList("java.lang.Object")));

        // Null should be ok
        wrapper = expectScriptThrows(ErrorCauseWrapper.class, () -> exec("Debug.explain(null)"));
        assertThat(wrapper.realCause.getClass(), equalTo(PainlessExplainError.class));
        e = (PainlessExplainError) wrapper.realCause;
        assertNull(e.getObjectToExplain());
        assertThat(e.getHeaders(painlessLookup), hasEntry("es.to_string", singletonList("null")));
        assertThat(e.getHeaders(painlessLookup), not(hasKey("es.java_class")));
        assertThat(e.getHeaders(painlessLookup), not(hasKey("es.painless_class")));

        // You can't catch the explain exception
        wrapper = expectScriptThrows(ErrorCauseWrapper.class, () -> exec("""
            try {
              Debug.explain(params.a)
            } catch (Exception e) {
              return 1
            }""", singletonMap("a", dummy), true));
        assertThat(wrapper.realCause.getClass(), equalTo(PainlessExplainError.class));
        e = (PainlessExplainError) wrapper.realCause;
        assertSame(dummy, e.getObjectToExplain());
    }

    /**
     * {@link PainlessExplainError} doesn't serialize but the headers still make it.
     */
    public void testPainlessExplainErrorSerialization() throws IOException {
        Map<String, Object> params = singletonMap("a", "jumped over the moon");
        ScriptException e = expectThrows(ScriptException.class, () -> exec("Debug.explain(params.a)", params, true));
        assertEquals(singletonList("jumped over the moon"), e.getMetadata("es.to_string"));
        assertEquals(singletonList("java.lang.String"), e.getMetadata("es.java_class"));
        assertEquals(singletonList("java.lang.String"), e.getMetadata("es.painless_class"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeException(e);
            try (StreamInput in = out.bytes().streamInput()) {
                ElasticsearchException read = (ScriptException) in.readException();
                assertEquals(singletonList("jumped over the moon"), read.getMetadata("es.to_string"));
                assertEquals(singletonList("java.lang.String"), read.getMetadata("es.java_class"));
                assertEquals(singletonList("java.lang.String"), read.getMetadata("es.painless_class"));
            }
        }
    }
}
