/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy.parser;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.yaml.YamlXContent;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class PolicyParserFailureTests extends ESTestCase {

    public void testParserSyntaxFailures() throws IOException {

        try (XContentBuilder builder = YamlXContent.contentBuilder()) {
            builder.startArray();
            builder.endArray();
            PolicyParserException ppe = expectThrows(PolicyParserException.class, () ->
                new PolicyParser("test-failure-policy.yaml",
                    new ByteArrayInputStream(Strings.toString(builder).getBytes())
                ).parsePolicy()
            );
            assertEquals("[1:5] policy parsing error for [test-failure-policy.yaml]: expected object [policy]", ppe.getMessage());
        }
    }
}
