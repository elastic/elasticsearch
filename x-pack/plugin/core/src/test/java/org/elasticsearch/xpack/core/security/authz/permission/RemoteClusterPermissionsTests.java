/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;

public class RemoteClusterPermissionsTests extends AbstractXContentSerializingTestCase<RemoteClusterPermissions> {

    List<String[]> groupPrivileges;
    List<String[]> groupClusters;
    RemoteClusterPermissions remoteClusterPermission;

    @Before
    void clean() {
        groupPrivileges = new ArrayList<>();
        groupClusters = new ArrayList<>();
        remoteClusterPermission = new RemoteClusterPermissions();
    }

    public void testToXContent() throws IOException {
        List<RemoteClusterPermissionGroup> groups = generateRandomGroups(false);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < groups.size(); i++) {
            String[] privileges = groupPrivileges.get(i);
            String[] clusters = groupClusters.get(i);
            sb.append(XContentHelper.stripWhitespace(String.format(Locale.ROOT, """
                    {
                      "privileges" : [
                        "%s"
                      ],
                      "clusters" : [
                        "%s"
                      ]
                    }
                """, String.join("\",\"", Arrays.asList(privileges)), String.join("\",\"", Arrays.asList(clusters)))));
            if (i < groups.size() - 1) {
                sb.append(",");
            }
        }
        String output = Strings.toString(remoteClusterPermission);
        assertEquals(XContentHelper.stripWhitespace(sb.toString()), XContentHelper.stripWhitespace(output));
    }

    public void testToString() throws IOException {
        for (int i = 0; i < generateRandomGroups(false).size(); i++) {
            String[] privileges = groupPrivileges.get(i);
            String[] clusters = groupClusters.get(i);
            assertThat(
                remoteClusterPermission.toString(),
                containsString("privileges=[" + String.join(", ", Arrays.asList(privileges)) + "]")
            );
            assertThat(remoteClusterPermission.toString(), containsString("clusters=[" + String.join(", ", Arrays.asList(clusters)) + "]"));
        }
    }

    public void testMatcher() {
        for (int i = 0; i < generateRandomGroups(true).size(); i++) {
            String[] clusters = groupClusters.get(i);
            for (String cluster : clusters) {
                assertTrue(remoteClusterPermission.hasPrivileges(cluster));
                assertFalse(remoteClusterPermission.hasPrivileges(randomAlphaOfLength(20)));
            }
        }
    }

    public void testPrivilegeNames() {
        for (int i = 0; i < generateRandomGroups(true).size(); i++) {
            String[] privileges = groupPrivileges.get(i);
            String[] clusters = groupClusters.get(i);
            for (String cluster : clusters) {
                Arrays.sort(privileges);
                String[] found = remoteClusterPermission.privilegeNames(cluster);
                Arrays.sort(found);
                assertArrayEquals(privileges, found);
            }
        }
    }

    public void testValidate() {
        generateRandomGroups(randomBoolean());
        // random values not allowed
        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> remoteClusterPermission.validate());
        assertTrue(error.getMessage().contains("Invalid remote_cluster permissions found. Please remove the remove the following:"));
        assertTrue(error.getMessage().contains("Only [monitor_enrich] is allowed"));

        new RemoteClusterPermissions().addGroup(new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "*" }))
            .validate(); // no error
    }

    private List<RemoteClusterPermissionGroup> generateRandomGroups(boolean fuzzyCluster) {
        clean();
        List<RemoteClusterPermissionGroup> groups = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            String[] privileges = generateRandomStringArray(5, 5, false, false);
            groupPrivileges.add(privileges);
            String[] clusters = generateRandomStringArray(5, 5, false, false);
            if (fuzzyCluster) {
                for (int j = 0; j < clusters.length; j++) {
                    if (randomBoolean()) {
                        clusters[j] = clusters[j].substring(0, clusters[j].length() - 1) + "*";
                    }
                }
            }
            groupClusters.add(clusters);
            RemoteClusterPermissionGroup group = new RemoteClusterPermissionGroup(privileges, clusters);
            groups.add(group);
            remoteClusterPermission.addGroup(group);
        }
        return groups;
    }

    @Override
    protected Writeable.Reader<RemoteClusterPermissions> instanceReader() {
        return RemoteClusterPermissions::new;
    }

    @Override
    protected RemoteClusterPermissions createTestInstance() {
        return new RemoteClusterPermissions().addGroup(
            new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "*" })
        );
    }

    @Override
    protected RemoteClusterPermissions mutateInstance(RemoteClusterPermissions instance) throws IOException {
        return new RemoteClusterPermissions().addGroup(
            new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "*" })
        ).addGroup(new RemoteClusterPermissionGroup(new String[] { "foobar" }, new String[] { "*" }));
    }

    @Override
    protected RemoteClusterPermissions doParseInstance(XContentParser parser) throws IOException {
        // fromXContent/parsing isn't supported since we still do old school manual parsing of the role descriptor
        return createTestInstance();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    RemoteClusterPermissionGroup.class,
                    RemoteClusterPermissionGroup.NAME,
                    RemoteClusterPermissionGroup::new
                )
            )
        );
    }
}
