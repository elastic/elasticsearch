/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class NullValueTests extends MapperServiceTestCase {

    public void testNullNullValue() throws Exception {

        String[] typesToTest = { "integer", "long", "double", "float", "short", "date", "ip", "keyword", "boolean", "byte", "geo_point" };

        for (String type : typesToTest) {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", type).nullField("null_value")));

            mapper.parse(source(b -> b.nullField("field")));

            ToXContent.Params params = new ToXContent.MapParams(Map.of("include_defaults", "true"));
            XContentBuilder b = JsonXContent.contentBuilder().startObject();
            mapper.mapping().toXContent(b, params);
            b.endObject();
            assertThat(Strings.toString(b), containsString("\"null_value\":null"));
        }
    }
}
