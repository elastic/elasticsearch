/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.script.Script;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ValueSourceMustacheIT extends AbstractScriptTestCase {

    public void testValueSourceWithTemplates() {
        Map<String, Object> model = new HashMap<>();
        model.put("field1", "value1");
        model.put("field2", Collections.singletonMap("field3", "value3"));

        ValueSource valueSource = ValueSource.wrap("{{field1}}/{{field2}}/{{field2.field3}}", scriptService);
        assertThat(valueSource, instanceOf(ValueSource.TemplatedValue.class));
        assertThat(valueSource.copyAndResolve(() -> model), equalTo("value1/{field3=value3}/value3"));

        valueSource = ValueSource.wrap(Arrays.asList("_value", "{{field1}}"), scriptService);
        assertThat(valueSource, instanceOf(ValueSource.ListValue.class));
        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) valueSource.copyAndResolve(() -> model);
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0), equalTo("_value"));
        assertThat(result.get(1), equalTo("value1"));

        Map<String, Object> map = new HashMap<>();
        map.put("field1", "{{field1}}");
        map.put("field2", Collections.singletonMap("field3", "{{field2.field3}}"));
        map.put("field4", "_value");
        valueSource = ValueSource.wrap(map, scriptService);
        assertThat(valueSource, instanceOf(ValueSource.MapValue.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> resultMap = (Map<String, Object>) valueSource.copyAndResolve(() -> model);
        assertThat(resultMap.size(), equalTo(3));
        assertThat(resultMap.get("field1"), equalTo("value1"));
        assertThat(((Map) resultMap.get("field2")).size(), equalTo(1));
        assertThat(((Map) resultMap.get("field2")).get("field3"), equalTo("value3"));
        assertThat(resultMap.get("field4"), equalTo("_value"));
    }

    public void testAccessSourceViaTemplate() {
        IngestDocument ingestDocument = new IngestDocument("marvel", "id", null, null, null, new HashMap<>());
        assertThat(ingestDocument.hasField("marvel"), is(false));
        ingestDocument.setFieldValue(compile("{{_index}}"), ValueSource.wrap("{{_index}}", scriptService));
        assertThat(ingestDocument.getFieldValue("marvel", String.class), equalTo("marvel"));
        ingestDocument.removeField(compile("{{marvel}}"));
        assertThat(ingestDocument.hasField("index"), is(false));
    }

    public void testWithConfigurableEncoders() {
        Map<String, Object> model = new HashMap<>();
        model.put("log_line", "10.10.1.1 - - [17/Nov/2020:04:59:43 +0000] \"GET /info HTTP/1.1\" 200 6229 \"-\" \"-\"  2");

        // default encoder should be application/json
        ValueSource valueSource = ValueSource.wrap("{{log_line}}", scriptService);
        Object result = valueSource.copyAndResolve(() -> model);
        assertThat(result,
            equalTo("10.10.1.1 - - [17/Nov/2020:04:59:43 +0000] \\\"GET /info HTTP/1.1\\\" 200 6229 \\\"-\\\" \\\"-\\\"  2"));

        // text/plain encoder
        var scriptOptions = Map.of(Script.CONTENT_TYPE_OPTION, "text/plain");
        valueSource = ValueSource.wrap("{{log_line}}", scriptService, scriptOptions);
        result = valueSource.copyAndResolve(() -> model);
        assertThat(result, equalTo("10.10.1.1 - - [17/Nov/2020:04:59:43 +0000] \"GET /info HTTP/1.1\" 200 6229 \"-\" \"-\"  2"));

        // application/x-www-form-urlencoded encoder
        scriptOptions = Map.of(Script.CONTENT_TYPE_OPTION, "application/x-www-form-urlencoded");
        valueSource = ValueSource.wrap("{{log_line}}", scriptService, scriptOptions);
        result = valueSource.copyAndResolve(() -> model);
        assertThat(result, equalTo("10.10.1.1+-+-+%5B17%2FNov%2F2020%3A04%3A59%3A43+%2B0000%5D+%22GET+%2Finfo+HTTP%2F1.1%22+200" +
            "+6229+%22-%22+%22-%22++2"));
    }
}
