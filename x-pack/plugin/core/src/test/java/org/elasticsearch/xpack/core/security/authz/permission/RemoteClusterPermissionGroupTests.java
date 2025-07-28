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
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class RemoteClusterPermissionGroupTests extends AbstractXContentSerializingTestCase<RemoteClusterPermissionGroup> {

    public void testToXContent() throws IOException {
        String[] privileges = generateRandomStringArray(5, 5, false, false);
        String[] clusters = generateRandomStringArray(5, 5, false, false);
        RemoteClusterPermissionGroup remoteClusterPermissionGroup = new RemoteClusterPermissionGroup(privileges, clusters);
        String output = Strings.toString(remoteClusterPermissionGroup);
        assertEquals(
            XContentHelper.stripWhitespace(String.format(Locale.ROOT, """
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
                clusters[i] = clusters[i].substring(0, clusters[i].length() - 1) + "*";
            }
        }
        RemoteClusterPermissionGroup remoteClusterPermissionGroup = new RemoteClusterPermissionGroup(privileges, clusters);
        for (String cluster : clusters) {
            assertTrue(remoteClusterPermissionGroup.hasPrivileges(cluster));
            assertFalse(remoteClusterPermissionGroup.hasPrivileges(randomAlphaOfLength(20)));
        }
    }

    public void testNullAndEmptyArgs() {
        final ThrowingRunnable nullGroup = randomFrom(
            () -> new RemoteClusterPermissionGroup(null, null),
            () -> new RemoteClusterPermissionGroup(new String[] {}, new String[] {}),
            () -> new RemoteClusterPermissionGroup(null, new String[] {}),
            () -> new RemoteClusterPermissionGroup(new String[] {}, null)
        );
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, nullGroup);
        assertEquals("remote cluster groups must not be null or empty", e.getMessage());
    }

    public void testInvalidValues() {
        final ThrowingRunnable invalidClusterAlias = randomFrom(
            () -> new RemoteClusterPermissionGroup(new String[] { "foo" }, new String[] { null }),
            () -> new RemoteClusterPermissionGroup(new String[] { "foo" }, new String[] { "bar", null }),
            () -> new RemoteClusterPermissionGroup(new String[] { "foo" }, new String[] { "bar", "" }),
            () -> new RemoteClusterPermissionGroup(new String[] { "foo" }, new String[] { "bar", " " })
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, invalidClusterAlias);
        assertThat(e.getMessage(), containsString("remote_cluster clusters aliases must contain valid non-empty, non-null values"));

        final ThrowingRunnable invalidPermission = randomFrom(
            () -> new RemoteClusterPermissionGroup(new String[] { null }, new String[] { "bar" }),
            () -> new RemoteClusterPermissionGroup(new String[] { "foo", null }, new String[] { "bar" }),
            () -> new RemoteClusterPermissionGroup(new String[] { "foo", "" }, new String[] { "bar" }),
            () -> new RemoteClusterPermissionGroup(new String[] { "foo", " " }, new String[] { "bar" })
        );

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, invalidPermission);
        assertThat(e2.getMessage(), containsString("remote_cluster privileges must contain valid non-empty, non-null values"));
    }

    public void testToMap() {
        String[] privileges = generateRandomStringArray(5, 5, false, false);
        String[] clusters = generateRandomStringArray(5, 5, false, false);
        RemoteClusterPermissionGroup remoteClusterPermissionGroup = new RemoteClusterPermissionGroup(privileges, clusters);
        assertEquals(
            Map.of("privileges", Arrays.asList(privileges), "clusters", Arrays.asList(clusters)),
            remoteClusterPermissionGroup.toMap()
        );
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
