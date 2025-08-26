/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class XContentParserTsidFunnelTests extends ESTestCase {

    public void testTsidFunnel() throws IOException {
        TsidBuilder xContentTsidBuilder = new TsidBuilder();
        xContentTsidBuilder.add(createParser(JsonXContent.jsonXContent, """
            {
              "string": "value",
              "int": 42,
              "long": 1234567890123,
              "double": 3.14159,
              "boolean": true,
              "null_value": null,
              "object": {
                "nested_string": "nested_value"
              },
              "array": ["elem1", "elem2", 3, 4.5, false]
            }
            """), XContentParserTsidFunnel.get());
        TsidBuilder manualTsidBuilder = TsidBuilder.newBuilder()
            .addStringDimension("string", "value")
            .addIntDimension("int", 42)
            .addLongDimension("long", 1234567890123L)
            .addDoubleDimension("double", 3.14159)
            .addBooleanDimension("boolean", true)
            .addStringDimension("object.nested_string", "nested_value")
            .addStringDimension("array", "elem1")
            .addStringDimension("array", "elem2")
            .addIntDimension("array", 3)
            .addDoubleDimension("array", 4.5)
            .addBooleanDimension("array", false);
        assertThat(xContentTsidBuilder.hash(), equalTo(manualTsidBuilder.hash()));
        assertThat(xContentTsidBuilder.buildTsid(), equalTo(manualTsidBuilder.buildTsid()));
    }

    public void testFilteredTsidFunnel() throws IOException {
        TsidBuilder xContentTsidBuilder = new TsidBuilder();
        xContentTsidBuilder.add(JsonXContent.jsonXContent.createParser(getFilteredConfig(Set.of("attributes.*")), """
            {
              "attributes": {
                "string": "value",
                "int": 42,
                "long": 1234567890123
              },
              "other_field": "should_not_be_included"
            }
            """), XContentParserTsidFunnel.get());
        TsidBuilder manualTsidBuilder = TsidBuilder.newBuilder()
            .addStringDimension("attributes.string", "value")
            .addIntDimension("attributes.int", 42)
            .addLongDimension("attributes.long", 1234567890123L);
        assertThat(xContentTsidBuilder.hash(), equalTo(manualTsidBuilder.hash()));
        assertThat(xContentTsidBuilder.buildTsid(), equalTo(manualTsidBuilder.buildTsid()));
    }

    public void testNoMatchingDimensions() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new TsidBuilder().add(JsonXContent.jsonXContent.createParser(getFilteredConfig(Set.of("attributes.*")), """
                {
                  "other_field": "should_not_be_included"
                }
                """), XContentParserTsidFunnel.get())
        );
        assertThat(e.getMessage(), equalTo("Error extracting tsid: source didn't contain any dimension fields"));
    }

    private static XContentParserConfiguration getFilteredConfig(Set<String> includePaths) {
        return XContentParserConfiguration.EMPTY.withFiltering(null, includePaths, null, true);
    }
}
