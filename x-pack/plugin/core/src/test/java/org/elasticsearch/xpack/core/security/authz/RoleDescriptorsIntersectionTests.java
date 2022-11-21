/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;
import static org.hamcrest.Matchers.equalTo;

public class RoleDescriptorsIntersectionTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final Collection<Set<RoleDescriptor>> originalSets = randomList(0, 3, () -> Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 3)));
        final RoleDescriptorsIntersection originalIntersection = new RoleDescriptorsIntersection(originalSets);

        final BytesReference serialized = originalIntersection.toBytesReference();
        final RoleDescriptorsIntersection deserialized = RoleDescriptorsIntersection.fromBytesReference(serialized);
        assertThat(deserialized, equalTo(originalIntersection));

        final Collection<Set<RoleDescriptor>> deserializedSets = deserialized.roleDescriptorsSets();
        assertThat(deserializedSets, equalTo(originalSets));
    }

    public void testXContent() throws IOException {
        final List<Set<RoleDescriptor>> originalSets = List.of(
            Set.of(new RoleDescriptor("role_0", new String[] { "monitor" }, null, null)),
            Set.of(new RoleDescriptor("role_1", new String[] { "all" }, null, null))
        );
        final RoleDescriptorsIntersection originalIntersection = new RoleDescriptorsIntersection(originalSets);
        final String jsonString = originalIntersection.toJson();

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

        final RoleDescriptorsIntersection decodedIntersection = RoleDescriptorsIntersection.fromJson(jsonString);
        final Collection<Set<RoleDescriptor>> decodedSets = decodedIntersection.roleDescriptorsSets();
        assertThat(decodedSets, equalTo(originalSets));
        assertThat(decodedIntersection, equalTo(originalIntersection));
    }
}
