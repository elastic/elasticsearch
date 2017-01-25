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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.script.ScriptException;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.hasEntry;

public class DebugTests extends ScriptTestCase {
    public void testExplain() {
        Object dummy = new Object();
        Map<String, Object> params = singletonMap("a", dummy);

        Debug.PainlessExplainError e = expectScriptThrows(Debug.PainlessExplainError.class, () -> exec(
                "Debug.explain(params.a)", params, true));
        assertSame(dummy, e.getObjectToExplain());
        assertThat(e.getMetadata(), hasEntry("es.class", singletonList("java.lang.Object")));
        assertThat(e.getMetadata(), hasEntry("es.to_string", singletonList(dummy.toString())));

        // Null should be ok
        e = expectScriptThrows(Debug.PainlessExplainError.class, () -> exec("Debug.explain(null)"));
        assertNull(e.getObjectToExplain());
        assertThat(e.getMetadata(), hasEntry("es.class", singletonList("null")));
        assertThat(e.getMetadata(), hasEntry("es.to_string", singletonList("null")));

        // You can't catch the explain exception
        e = expectScriptThrows(Debug.PainlessExplainError.class, () -> exec(
                "try {\n"
              + "  Debug.explain(params.a)\n"
              + "} catch (Exception e) {\n"
              + "  return 1\n"
              + "}", params, true));
        assertSame(dummy, e.getObjectToExplain());
    }

    /**
     * {@link Debug.PainlessExplainError} doesn't serialize but the headers still make it.
     */
    public void testPainlessExplainErrorSerialization() throws IOException {
        Map<String, Object> params = singletonMap("a", "jumped over the moon");
        ScriptException e = expectThrows(ScriptException.class, () -> exec("Debug.explain(params.a)", params, true));
        assertEquals(singletonList("java.lang.String"), e.getMetadata("es.class"));
        assertEquals(singletonList("jumped over the moon"), e.getMetadata("es.to_string"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeException(e);
            try (StreamInput in = out.bytes().streamInput()) {
                ElasticsearchException read = (ScriptException) in.readException();
                assertEquals(singletonList("java.lang.String"), read.getMetadata("es.class"));
                assertEquals(singletonList("jumped over the moon"), read.getMetadata("es.to_string"));
            }
        }
    }
}
