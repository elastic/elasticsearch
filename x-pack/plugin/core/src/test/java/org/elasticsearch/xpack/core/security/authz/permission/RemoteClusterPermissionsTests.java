/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions.ROLE_REMOTE_CLUSTER_PRIVS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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
        Map<TransportVersion, Set<String>> original = RemoteClusterPermissions.allowedRemoteClusterPermissions;
        try {
            // create random groups with random privileges for random clusters
            List<RemoteClusterPermissionGroup> randomGroups = generateRandomGroups(true);
            RemoteClusterPermissions.allowedRemoteClusterPermissions = new HashMap<>();
            Set<String> allPrivileges = new HashSet<>();
            // allow all the privileges across the random groups for the current version
            for (int i = 0; i < randomGroups.size(); i++) {
                allPrivileges.addAll(Set.of(groupPrivileges.get(i)));
            }
            RemoteClusterPermissions.allowedRemoteClusterPermissions.put(TransportVersion.current(), allPrivileges);

            for (int i = 0; i < randomGroups.size(); i++) {
                String[] privileges = groupPrivileges.get(i);
                String[] clusters = groupClusters.get(i);
                for (String cluster : clusters) {
                    String[] found = remoteClusterPermission.privilegeNames(cluster, TransportVersion.current());
                    Arrays.sort(found);
                    // ensure all lowercase since the privilege names are case insensitive and the method will result in lowercase
                    for (int j = 0; j < privileges.length; j++) {
                        privileges[j] = privileges[j].toLowerCase(Locale.ROOT);
                    }
                    Arrays.sort(privileges);
                    // the two array are always equal since the all the random values are allowed
                    assertArrayEquals(privileges, found);
                }
            }
        } finally {
            RemoteClusterPermissions.allowedRemoteClusterPermissions = original;
        }

        // create random groups with random privileges for random clusters
        List<RemoteClusterPermissionGroup> randomGroups = generateRandomGroups(true);
        // replace a random value with one that is allowed
        groupPrivileges.get(0)[0] = "monitor_enrich";

        for (int i = 0; i < randomGroups.size(); i++) {
            String[] privileges = groupPrivileges.get(i);
            String[] clusters = groupClusters.get(i);
            for (String cluster : clusters) {
                String[] found = remoteClusterPermission.privilegeNames(cluster, TransportVersion.current());
                Arrays.sort(found);
                // ensure all lowercase since the privilege names are case insensitive and the method will result in lowercase
                for (int j = 0; j < privileges.length; j++) {
                    privileges[j] = privileges[j].toLowerCase(Locale.ROOT);
                }
                Arrays.sort(privileges);

                // the results are conditional. the first group has a value that is allowed for the current version
                if (i == 0 && privileges.length == 1) {
                    // special case where there was only 1 random value and it was replaced with a value that is allowed
                    assertArrayEquals(privileges, found);
                } else {
                    // none of the random privileges are allowed for the current version
                    assertFalse(Arrays.equals(privileges, found));
                    if (i == 0) {
                        // ensure that for the current version we only find the valid "monitor_enrich"
                        assertThat(Set.of(found), equalTo(Set.of("monitor_enrich")));
                    } else {
                        // all other groups should be found to not have any privileges
                        assertTrue(found.length == 0);
                    }
                }
            }
        }
    }

    public void testMonitorEnrichPerVersion() {
        // test monitor_enrich before, after and on monitor enrich version
        String[] privileges = randomBoolean() ? new String[] { "monitor_enrich" } : new String[] { "monitor_enrich", "foo", "bar" };
        String[] before = new RemoteClusterPermissions().addGroup(new RemoteClusterPermissionGroup(privileges, new String[] { "*" }))
            .privilegeNames("*", TransportVersionUtils.getPreviousVersion(ROLE_REMOTE_CLUSTER_PRIVS));
        // empty set since monitor_enrich is not allowed in the before version
        assertThat(Set.of(before), equalTo(Collections.emptySet()));
        String[] on = new RemoteClusterPermissions().addGroup(new RemoteClusterPermissionGroup(privileges, new String[] { "*" }))
            .privilegeNames("*", ROLE_REMOTE_CLUSTER_PRIVS);
        // only monitor_enrich since the other values are not allowed
        assertThat(Set.of(on), equalTo(Set.of("monitor_enrich")));
        String[] after = new RemoteClusterPermissions().addGroup(new RemoteClusterPermissionGroup(privileges, new String[] { "*" }))
            .privilegeNames("*", TransportVersion.current());
        // only monitor_enrich since the other values are not allowed
        assertThat(Set.of(after), equalTo(Set.of("monitor_enrich")));
    }

    public void testValidate() {
        generateRandomGroups(randomBoolean());
        // random values not allowed
        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> remoteClusterPermission.validate());
        assertTrue(error.getMessage().contains("Invalid remote_cluster permissions found. Please remove the following:"));
        assertTrue(error.getMessage().contains("Only [monitor_enrich] are allowed"));

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
