/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.Restriction;
import org.elasticsearch.xpack.core.security.authz.restriction.WorkflowResolver;

import java.io.IOException;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RoleRestrictionTests extends ESTestCase {

    public void testParse() throws Exception {
        final String json = """
            {
                "workflows": ["search_application_query"]
            }
            """;
        Restriction r = Restriction.parse("test_restriction", createJsonParser(json));
        assertThat(r.getWorkflows(), arrayContaining("search_application_query"));
        assertThat(r.hasWorkflows(), equalTo(true));
        assertThat(r.isEmpty(), equalTo(false));

        // tests that "restriction": {} is allowed
        r = Restriction.parse("test_restriction", createJsonParser("{}"));
        assertThat(r.hasWorkflows(), equalTo(false));
        assertThat(r.isEmpty(), equalTo(true));

        var e = expectThrows(ElasticsearchParseException.class, () -> Restriction.parse("test_restriction", createJsonParser("""
             {
                 "workflows": []
             }
            """)));
        assertThat(
            e.getMessage(),
            containsString("failed to parse restriction for role [test_restriction]. [workflows] cannot be an empty array")
        );

        e = expectThrows(ElasticsearchParseException.class, () -> Restriction.parse("test_restriction", createJsonParser("""
             {
                 "workflows": null
             }
            """)));
        assertThat(
            e.getMessage(),
            containsString(
                "failed to parse restriction for role [test_restriction]. could not parse [workflows] field. "
                    + "expected a string array but found null value instead"
            )
        );
    }

    public void testToXContent() throws Exception {
        final Restriction restriction = randomWorkflowsRestriction(1, 5);
        final XContentType xContentType = randomFrom(XContentType.values());
        final BytesReference xContentValue = toShuffledXContent(restriction, xContentType, ToXContent.EMPTY_PARAMS, false);
        try (XContentParser parser = xContentType.xContent().createParser(XContentParserConfiguration.EMPTY, xContentValue.streamInput())) {
            final Restriction parsed = Restriction.parse(randomAlphaOfLengthBetween(3, 6), parser);
            assertThat(parsed, equalTo(restriction));
        }
    }

    public void testSerialization() throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        final Restriction original = randomWorkflowsRestriction(1, 3);
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        final Restriction actual = new Restriction(in);

        assertThat(actual, equalTo(original));
    }

    public void testIsEmpty() {
        String[] workflows = null;
        Restriction r = new Restriction(workflows);
        assertThat(r.isEmpty(), equalTo(true));
        assertThat(r.hasWorkflows(), equalTo(false));

        workflows = randomWorkflowNames(1, 2);
        r = new Restriction(workflows);
        assertThat(r.isEmpty(), equalTo(false));
        assertThat(r.hasWorkflows(), equalTo(true));
    }

    private static XContentParser createJsonParser(String json) throws IOException {
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, new BytesArray(json).streamInput());
    }

    public static Restriction randomWorkflowsRestriction(int min, int max) {
        return new Restriction(randomWorkflowNames(min, max));
    }

    public static String[] randomWorkflowNames(int min, int max) {
        return randomArray(min, max, String[]::new, () -> randomFrom(WorkflowResolver.allWorkflows()).name());
    }
}
