/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;

public class RemoteClusterPermissionGroupTests extends AbstractXContentSerializingTestCase<RemoteClusterPermissionGroup> {

    public void testToXContent() throws IOException {
        String[] privileges = generateRandomStringArray(5, 5, false, false);
        String[] clusters = generateRandomStringArray(5, 5, false, false);
        RemoteClusterPermissionGroup remoteClusterPermissionGroup = new RemoteClusterPermissionGroup(privileges, clusters);
        String output = Strings.toString(remoteClusterPermissionGroup);
        assertEquals(
            XContentHelper.stripWhitespace(String.format("""
                    {
                      "privileges" : [
                        "%s"
                      ],
                      "clusters" : [
                        "%s"
                      ]
                    }
                """, String.join("\",\"", Arrays.asList(privileges)), String.join("\",\"", Arrays.asList(clusters)))),
            XContentHelper.stripWhitespace(output)
        );
    }

    public void testToString() throws IOException {
        String[] privileges = generateRandomStringArray(5, 5, false, false);
        String[] clusters = generateRandomStringArray(5, 5, false, false);
        RemoteClusterPermissionGroup remoteClusterPermissionGroup = new RemoteClusterPermissionGroup(privileges, clusters);
        assertThat(
            remoteClusterPermissionGroup.toString(),
            containsString("privileges=[" + String.join(", ", Arrays.asList(privileges)) + "]")
        );
        assertThat(
            remoteClusterPermissionGroup.toString(),
            containsString("clusters=[" + String.join(", ", Arrays.asList(clusters)) + "]")
        );
    }

    public void testMatcher() {
        String[] privileges = generateRandomStringArray(5, 5, false, false);
        String[] clusters = generateRandomStringArray(5, 5, false, false);
        for (int i = 0; i < clusters.length; i++) {
            if (randomBoolean()) {
                clusters[i] = clusters[i] + "*";
            }
        }
        RemoteClusterPermissionGroup remoteClusterPermissionGroup = new RemoteClusterPermissionGroup(privileges, clusters);
        for (String cluster : clusters) {
            assertTrue(remoteClusterPermissionGroup.hasPrivileges(cluster));
            assertFalse(remoteClusterPermissionGroup.hasPrivileges(randomAlphaOfLength(20)));
        }
    }

    @Override
    protected Writeable.Reader<RemoteClusterPermissionGroup> instanceReader() {
        return RemoteClusterPermissionGroup::new;
    }

    @Override
    protected RemoteClusterPermissionGroup createTestInstance() {
        return new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "*" });
    }

    @Override
    protected RemoteClusterPermissionGroup mutateInstance(RemoteClusterPermissionGroup instance) throws IOException {
        if (randomBoolean()) {
            return new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "foo", "bar" });
        } else {
            return new RemoteClusterPermissionGroup(new String[] { "foobar" }, new String[] { "*" });
        }
    }

    @Override
    protected RemoteClusterPermissionGroup doParseInstance(XContentParser parser) throws IOException {
        // fromXContent/parsing isn't supported since we still do old school manual parsing of the role descriptor
        return createTestInstance();
    }

}
