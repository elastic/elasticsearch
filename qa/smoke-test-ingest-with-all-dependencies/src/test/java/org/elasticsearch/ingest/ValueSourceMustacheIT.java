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

package org.elasticsearch.ingest;

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
        assertThat(valueSource.copyAndResolve(model), equalTo("value1/{field3=value3}/value3"));

        valueSource = ValueSource.wrap(Arrays.asList("_value", "{{field1}}"), scriptService);
        assertThat(valueSource, instanceOf(ValueSource.ListValue.class));
        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) valueSource.copyAndResolve(model);
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
        Map<String, Object> resultMap = (Map<String, Object>) valueSource.copyAndResolve(model);
        assertThat(resultMap.size(), equalTo(3));
        assertThat(resultMap.get("field1"), equalTo("value1"));
        assertThat(((Map) resultMap.get("field2")).size(), equalTo(1));
        assertThat(((Map) resultMap.get("field2")).get("field3"), equalTo("value3"));
        assertThat(resultMap.get("field4"), equalTo("_value"));
    }

    public void testAccessSourceViaTemplate() {
        IngestDocument ingestDocument = new IngestDocument("marvel", "type", "id", null, null, null, new HashMap<>());
        assertThat(ingestDocument.hasField("marvel"), is(false));
        ingestDocument.setFieldValue(compile("{{_index}}"), ValueSource.wrap("{{_index}}", scriptService));
        assertThat(ingestDocument.getFieldValue("marvel", String.class), equalTo("marvel"));
        ingestDocument.removeField(compile("{{marvel}}"));
        assertThat(ingestDocument.hasField("index"), is(false));
    }

}
