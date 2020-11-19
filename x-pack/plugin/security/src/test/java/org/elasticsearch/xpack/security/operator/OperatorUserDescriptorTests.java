/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

public class OperatorUserDescriptorTests extends ESTestCase {

    public void testParseConfig() throws IOException {
        final String config = ""
            + "operator:\n"
            + "  - usernames: [\"operator_1\",\"operator_2\"]\n"
            + "    realm_name: \"found\"\n"
            + "    realm_type: \"file\"\n"
            + "    auth_type: \"REALM\"\n"
            + "  - usernames: [\"internal_system\"]\n";

        try (ByteArrayInputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8))) {
            final List<OperatorUserDescriptor.Group> groups = OperatorUserDescriptor.parseConfig(in);
            assertEquals(2, groups.size());
            System.out.println(groups);
            assertEquals(new OperatorUserDescriptor.Group(Set.of("operator_1", "operator_2"), "found"), groups.get(0));
            assertEquals(new OperatorUserDescriptor.Group(Set.of("internal_system")), groups.get(1));
        }
    }
}
