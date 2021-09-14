/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.explain;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;

public class ExplainRequestTests extends ESTestCase {
    private NamedWriteableRegistry namedWriteableRegistry;

    public void setUp() throws Exception {
        super.setUp();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(IndicesModule.getNamedWriteables());
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    public void testSerialize() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            ExplainRequest request = new ExplainRequest("index", "id");
            request.fetchSourceContext(new FetchSourceContext(true, new String[]{"field1.*"}, new String[] {"field2.*"}));
            request.filteringAlias(new AliasFilter(QueryBuilders.termQuery("filter_field", "value"), "alias0", "alias1"));
            request.preference("the_preference");
            request.query(QueryBuilders.termQuery("field", "value"));
            request.storedFields(new String[] {"field1", "field2"});
            request.routing("some_routing");
            request.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                ExplainRequest readRequest = new ExplainRequest(in);
                assertEquals(request.filteringAlias(), readRequest.filteringAlias());
                assertArrayEquals(request.storedFields(), readRequest.storedFields());
                assertEquals(request.preference(), readRequest.preference());
                assertEquals(request.query(), readRequest.query());
                assertEquals(request.routing(), readRequest.routing());
                assertEquals(request.fetchSourceContext(), readRequest.fetchSourceContext());
            }
        }
    }

    public void testValidation() {
        {
            final ExplainRequest request = new ExplainRequest("index4", "0");
            request.query(QueryBuilders.termQuery("field", "value"));

            final ActionRequestValidationException validate = request.validate();

            assertThat(validate, nullValue());
        }

        {
            final ExplainRequest request = new ExplainRequest("index4", randomBoolean() ? "" : null);
            request.query(QueryBuilders.termQuery("field", "value"));
            final ActionRequestValidationException validate = request.validate();

            assertThat(validate, not(nullValue()));
            assertThat(validate.validationErrors(), hasItems("id is missing"));
        }
    }
}
