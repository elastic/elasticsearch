/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DotExpanderProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        DotExpanderProcessor.Factory factory = new DotExpanderProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field.field");
        config.put("path", "_path");
        DotExpanderProcessor processor = (DotExpanderProcessor) factory.create(null, "_tag", null, config);
        assertThat(processor.getField(), equalTo("_field.field"));
        assertThat(processor.getPath(), equalTo("_path"));

        config = new HashMap<>();
        config.put("field", "_field.field");
        processor = (DotExpanderProcessor) factory.create(null, "_tag", null, config);
        assertThat(processor.getField(), equalTo("_field.field"));
        assertThat(processor.getPath(), nullValue());
    }

    public void testValidFields() throws Exception {
        DotExpanderProcessor.Factory factory = new DotExpanderProcessor.Factory();

        String[] fields = new String[] {"a.b", "a.b.c", "a.b.c.d", "ab.cd"};
        for (String field : fields) {
            Map<String, Object> config = new HashMap<>();
            config.put("field", field);
            config.put("path", "_path");
            DotExpanderProcessor processor = (DotExpanderProcessor) factory.create(null, "_tag", null, config);
            assertThat(processor.getField(), equalTo(field));
            assertThat(processor.getPath(), equalTo("_path"));
        }
    }

    public void testCreate_fieldMissing() throws Exception {
        DotExpanderProcessor.Factory factory = new DotExpanderProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("path", "_path");
        Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, "_tag", null, config));
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreate_invalidFields() throws Exception {
        DotExpanderProcessor.Factory factory = new DotExpanderProcessor.Factory();
        String[] fields = new String[] {"a", "abc"};
        for (String field : fields) {
            Map<String, Object> config = new HashMap<>();
            config.put("field", field);
            Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, "_tag", null, config));
            assertThat(e.getMessage(), equalTo("[field] field does not contain a dot and is not a wildcard"));
        }

        fields = new String[] {".a", "a.", "."};
        for (String field : fields) {
            Map<String, Object> config = new HashMap<>();
            config.put("field", field);
            Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, "_tag", null, config));
            assertThat(e.getMessage(), equalTo("[field] Field can't start or end with a dot"));
        }

        fields = new String[] {"a..b", "a...b", "a.b..c", "abc.def..hij"};
        for (String field : fields) {
            Map<String, Object> config = new HashMap<>();
            config.put("field", field);
            Exception e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, "_tag", null, config));
            assertThat(e.getMessage(), equalTo("[field] No space between dots"));
        }
    }

}
