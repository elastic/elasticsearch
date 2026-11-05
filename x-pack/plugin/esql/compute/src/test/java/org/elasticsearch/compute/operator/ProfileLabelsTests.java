/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ProfileLabelsTests extends ESTestCase {

    public void testPlainDescriptionIsJustAKind() {
        for (String description : new String[] { "data", "node_reduce", "final", "single" }) {
            assertThat(ProfileLabels.kind(description), equalTo(description));
            assertThat(ProfileLabels.branch(description), nullValue());
        }
    }

    public void testBranchesAreThePathBeforeTheKind() {
        assertThat(ProfileLabels.kind("subplan-0.merge"), equalTo("merge"));
        assertThat(ProfileLabels.branch("subplan-0.merge"), equalTo("subplan-0"));
        assertThat(ProfileLabels.kind("subplan-1.subplan-2.final"), equalTo("final"));
        assertThat(ProfileLabels.branch("subplan-1.subplan-2.final"), equalTo("subplan-1.subplan-2"));
        assertThat(ProfileLabels.kind("main.final"), equalTo("final"));
        assertThat(ProfileLabels.branch("main.final"), equalTo("main"));
    }

    public void testWritesWhatItKnows() throws IOException {
        assertThat(write("data", null, null), equalTo("{\"kind\":\"data\"}"));
        assertThat(write("subplan-0.merge", null, null), equalTo("{\"kind\":\"merge\",\"branch\":\"subplan-0\"}"));
        assertThat(
            write("final", "subplan-2", "inline_join"),
            equalTo("{\"kind\":\"final\",\"phase\":\"subplan-2\",\"phase_kind\":\"inline_join\"}")
        );
        // The phase and the branch both count subplans from zero, so they are separate fields
        assertThat(
            write("subplan-0.final", "subplan-1", "semi_join"),
            equalTo("{\"kind\":\"final\",\"branch\":\"subplan-0\",\"phase\":\"subplan-1\",\"phase_kind\":\"semi_join\"}")
        );
    }

    /** A description from a node too old to send one has nothing to describe. */
    public void testEmptyDescriptionWritesNoKind() throws IOException {
        assertThat(write("", null, null), equalTo("{}"));
    }

    private static String write(String description, String phase, String phaseKind) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            ProfileLabels.write(builder, description, phase, phaseKind);
            builder.endObject();
            return Strings.toString(builder);
        }
    }
}
