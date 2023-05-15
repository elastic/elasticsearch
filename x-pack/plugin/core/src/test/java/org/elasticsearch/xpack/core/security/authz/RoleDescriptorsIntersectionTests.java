/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;
import static org.hamcrest.Matchers.equalTo;

public class RoleDescriptorsIntersectionTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final RoleDescriptorsIntersection roleDescriptorsIntersection = new RoleDescriptorsIntersection(
            randomList(0, 3, () -> Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 3)))
        );

        final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    ConfigurableClusterPrivilege.class,
                    ConfigurableClusterPrivileges.ManageApplicationPrivileges.WRITEABLE_NAME,
                    ConfigurableClusterPrivileges.ManageApplicationPrivileges::createFrom
                ),
                new NamedWriteableRegistry.Entry(
                    ConfigurableClusterPrivilege.class,
                    ConfigurableClusterPrivileges.WriteProfileDataPrivileges.WRITEABLE_NAME,
                    ConfigurableClusterPrivileges.WriteProfileDataPrivileges::createFrom
                )
            )
        );

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            roleDescriptorsIntersection.writeTo(output);
            try (StreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                RoleDescriptorsIntersection deserialized = new RoleDescriptorsIntersection(input);
                assertThat(deserialized.roleDescriptorsList(), equalTo(roleDescriptorsIntersection.roleDescriptorsList()));
            }
        }
    }

    public void testXContent() throws IOException {
        final RoleDescriptorsIntersection roleDescriptorsIntersection = new RoleDescriptorsIntersection(
            List.of(
                Set.of(new RoleDescriptor("role_0", new String[] { "monitor" }, null, null)),
                Set.of(new RoleDescriptor("role_1", new String[] { "all" }, null, null))
            )
        );

        final XContentBuilder builder = XContentFactory.jsonBuilder();
        roleDescriptorsIntersection.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final String jsonString = Strings.toString(builder);

        assertThat(jsonString, equalTo(XContentHelper.stripWhitespace("""
            [
              {
                "role_0": {
                  "cluster": ["monitor"],
                  "indices": [],
                  "applications": [],
                  "run_as": [],
                  "metadata": {},
                  "transient_metadata": {"enabled": true}
                }
              },
              {
                "role_1": {
                  "cluster": ["all"],
                  "indices": [],
                  "applications": [],
                  "run_as": [],
                  "metadata": {},
                  "transient_metadata": {"enabled": true}
                }
              }
            ]""")));

        try (XContentParser p = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, jsonString)) {
            assertThat(RoleDescriptorsIntersection.fromXContent(p), equalTo(roleDescriptorsIntersection));
        }
    }
}
