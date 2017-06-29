/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.deprecation.DeprecationChecks.INDEX_SETTINGS_CHECKS;
import static org.hamcrest.core.IsEqual.equalTo;

public class DeprecationChecksTests extends ESTestCase {

    public void testFilterChecks() throws IOException {
        DeprecationIssue issue = DeprecationIssueTests.createTestInstance();
        int numChecksPassed = randomIntBetween(0, 5);
        int numChecksFailed = 10 - numChecksPassed;
        List<Supplier<DeprecationIssue>> checks = new ArrayList<>();
        for (int i = 0; i < numChecksFailed; i++) {
            checks.add(() -> issue);
        }
        for (int i = 0; i < numChecksPassed; i++) {
            checks.add(() -> null);
        }
        List<DeprecationIssue> filteredIssues = DeprecationChecks.filterChecks(checks, Supplier::get);
        assertThat(filteredIssues.size(), equalTo(numChecksFailed));
    }

    public void testCoerceBooleanDeprecation() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject(); {
            mapping.startObject("properties"); {
                mapping.startObject("my_boolean"); {
                    mapping.field("type", "boolean");
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        IndexMetaData indexMetaData = IndexMetaData.builder("test")
            .putMapping("testBooleanCoercion", mapping.string())
            .settings(settings(Version.V_5_6_0))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        DeprecationIssue expected = new DeprecationIssue(DeprecationIssue.Level.INFO,
            "Coercion of boolean fields",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/" +
                "breaking_60_mappings_changes.html#_coercion_of_boolean_fields",
            Arrays.toString(new String[] { "type: testBooleanCoercion, field: my_boolean" }));
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(INDEX_SETTINGS_CHECKS, c -> c.apply(indexMetaData));
        assertThat(issues.size(), equalTo(1));
        assertThat(issues.get(0), equalTo(expected));
    }
}
