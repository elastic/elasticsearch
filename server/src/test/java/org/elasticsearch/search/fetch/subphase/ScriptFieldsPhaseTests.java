/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptFieldsPhaseTests extends ESTestCase {

    public void testUnserializableXContent() throws Exception {
        class NonXContentable implements Writeable {
            public void writeTo(StreamOutput output) throws IOException {}
        }

        assertUnserializable(new FieldScript() {
            @Override
            public Object execute() {
                return new NonXContentable();
            }
        });
    }

    public void testUnserializableTransport() throws Exception {
        class NonWriteable implements ToXContent {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder;
            }
        }

        assertUnserializable(new FieldScript() {
            @Override
            public Object execute() {
                return new NonWriteable();
            }
        });
    }

    public void testUnserializableSelfReference() throws Exception {
        var map = new HashMap<String, Object>();
        map.put("foo", map);
        assertUnserializable(new FieldScript() {
            @Override
            public Object execute() {
                return map;
            }
        });
    }

    private void assertUnserializable(FieldScript script) throws Exception {
        ScriptFieldsPhase phase = new ScriptFieldsPhase();

        var fetchContext = mock(FetchContext.class);
        var scriptFields = new ScriptFieldsContext();
        FieldScript.LeafFactory scriptFactory = context -> script;
        scriptFields.add(new ScriptFieldsContext.ScriptField("field", scriptFactory, false));
        when(fetchContext.scriptFields()).thenReturn(scriptFields);

        FetchSubPhaseProcessor processor = phase.getProcessor(fetchContext);
        processor.setNextReader(null);
        var searchHit = new SearchHit(1);
        var hitContext = new FetchSubPhase.HitContext(searchHit, null, 1, Map.of(), null);
        processor.process(hitContext);

        DocumentField field = searchHit.field("field");
        assertThat(field.getValue(), equalTo("<unserializable>"));
    }
}
