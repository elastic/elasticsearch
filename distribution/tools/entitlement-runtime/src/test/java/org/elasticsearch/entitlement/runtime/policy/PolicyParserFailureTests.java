/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class PolicyParserFailureTests extends ESTestCase {

    public void testParserSyntaxFailures() throws IOException {
        try (XContentBuilder builder = YamlXContent.contentBuilder()) {
            builder.startArray();
            builder.endArray();
            PolicyParserException ppe = expectThrows(
                PolicyParserException.class,
                () -> new PolicyParser(
                    new ByteArrayInputStream(Strings.toString(builder).getBytes(StandardCharsets.UTF_8)),
                    "test-failure-policy.yaml"
                ).parsePolicy()
            );
            assertEquals("[1:5] policy parsing error for [test-failure-policy.yaml]: expected object <scope name>", ppe.getMessage());
        }
    }

    public void testEntitlementDoesNotExist() throws IOException {
        try (XContentBuilder builder = YamlXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("entitlement-module-1");
            builder.startArray("entitlements");
            builder.startObject();
            builder.startObject("does_not_exist");
            builder.endObject();
            builder.endObject();
            builder.endArray();
            builder.endObject();
            builder.endObject();
            PolicyParserException ppe = expectThrows(
                PolicyParserException.class,
                () -> new PolicyParser(
                    new ByteArrayInputStream(Strings.toString(builder).getBytes(StandardCharsets.UTF_8)),
                    "test-failure-policy.yaml"
                ).parsePolicy()
            );
            assertEquals(
                "[4:5] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-1]: "
                    + "unknown entitlement type [does_not_exist]",
                ppe.getMessage()
            );
        }
    }

    public void testEntitlementMissingParameter() throws IOException {
        try (XContentBuilder builder = YamlXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("entitlement-module-1");
            builder.startArray("entitlements");
            builder.startObject();
            builder.startObject("file");
            builder.endObject();
            builder.endObject();
            builder.endArray();
            builder.endObject();
            builder.endObject();
            PolicyParserException ppe = expectThrows(
                PolicyParserException.class,
                () -> new PolicyParser(
                    new ByteArrayInputStream(Strings.toString(builder).getBytes(StandardCharsets.UTF_8)),
                    "test-failure-policy.yaml"
                ).parsePolicy()
            );
            assertEquals(
                "[4:12] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-1] "
                    + "for entitlement type [file]: missing entitlement parameter [path]",
                ppe.getMessage()
            );
        }

        try (XContentBuilder builder = YamlXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("entitlement-module-1");
            builder.startArray("entitlements");
            builder.startObject();
            builder.startObject("file");
            builder.field("path", "test-path");
            builder.endObject();
            builder.endObject();
            builder.endArray();
            builder.endObject();
            builder.endObject();
            PolicyParserException ppe = expectThrows(
                PolicyParserException.class,
                () -> new PolicyParser(
                    new ByteArrayInputStream(Strings.toString(builder).getBytes(StandardCharsets.UTF_8)),
                    "test-failure-policy.yaml"
                ).parsePolicy()
            );
            assertEquals(
                "[6:1] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-1] "
                    + "for entitlement type [file]: missing entitlement parameter [actions]",
                ppe.getMessage()
            );
        }
    }

    public void testEntitlementExtraneousParameter() throws IOException {
        try (XContentBuilder builder = YamlXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("entitlement-module-1");
            builder.startArray("entitlements");
            builder.startObject();
            builder.startObject("file");
            builder.field("path", "test-path");
            builder.startArray("actions");
            builder.value("read");
            builder.endArray();
            builder.field("extra", "test");
            builder.endObject();
            builder.endObject();
            builder.endArray();
            builder.endObject();
            builder.endObject();
            PolicyParserException ppe = expectThrows(
                PolicyParserException.class,
                () -> new PolicyParser(
                    new ByteArrayInputStream(Strings.toString(builder).getBytes(StandardCharsets.UTF_8)),
                    "test-failure-policy.yaml"
                ).parsePolicy()
            );
            assertEquals(
                "[9:1] policy parsing error for [test-failure-policy.yaml] in scope [entitlement-module-1] "
                    + "for entitlement type [file]: extraneous entitlement parameter(s) {extra=test}",
                ppe.getMessage()
            );
        }
    }
}
