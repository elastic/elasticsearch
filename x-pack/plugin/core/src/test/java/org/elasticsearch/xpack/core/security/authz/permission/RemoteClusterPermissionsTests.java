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
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;
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
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions.ROLE_MONITOR_STATS;
import static org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions.ROLE_REMOTE_CLUSTER_PRIVS;
import static org.elasticsearch.xpack.core.security.authz.permission.RemoteClusterPermissions.lastTransportVersionPermission;
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
                assertTrue(remoteClusterPermission.hasAnyPrivileges(cluster));
                assertFalse(remoteClusterPermission.hasAnyPrivileges(randomAlphaOfLength(20)));
            }
        }
    }

    public void testCollapseAndRemoveUnsupportedPrivileges() {
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
                    String[] found = remoteClusterPermission.collapseAndRemoveUnsupportedPrivileges(cluster, TransportVersion.current());
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
        String singleValidPrivilege = randomFrom(
            RemoteClusterPermissions.allowedRemoteClusterPermissions.get(lastTransportVersionPermission)
        );
        groupPrivileges.get(0)[0] = singleValidPrivilege;

        for (int i = 0; i < randomGroups.size(); i++) {
            String[] privileges = groupPrivileges.get(i);
            String[] clusters = groupClusters.get(i);
            for (String cluster : clusters) {
                String[] found = remoteClusterPermission.collapseAndRemoveUnsupportedPrivileges(cluster, TransportVersion.current());
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
                        assertThat(Set.of(found), equalTo(Set.of(singleValidPrivilege)));
                    } else {
                        // all other groups should be found to not have any privileges
                        assertTrue(found.length == 0);
                    }
                }
            }
        }
    }

    public void testPermissionsPerVersion() {
        testPermissionPerVersion("monitor_enrich", ROLE_REMOTE_CLUSTER_PRIVS);
        testPermissionPerVersion("monitor_stats", ROLE_MONITOR_STATS);
    }

    private void testPermissionPerVersion(String permission, TransportVersion version) {
        // test permission before, after and on the version
        String[] privileges = randomBoolean() ? new String[] { permission } : new String[] { permission, "foo", "bar" };
        String[] before = new RemoteClusterPermissions().addGroup(new RemoteClusterPermissionGroup(privileges, new String[] { "*" }))
            .collapseAndRemoveUnsupportedPrivileges("*", TransportVersionUtils.getPreviousVersion(version));
        // empty set since permissions is not allowed in the before version
        assertThat(Set.of(before), equalTo(Collections.emptySet()));
        String[] on = new RemoteClusterPermissions().addGroup(new RemoteClusterPermissionGroup(privileges, new String[] { "*" }))
            .collapseAndRemoveUnsupportedPrivileges("*", version);
        // the permission is found on that provided version
        assertThat(Set.of(on), equalTo(Set.of(permission)));
        String[] after = new RemoteClusterPermissions().addGroup(new RemoteClusterPermissionGroup(privileges, new String[] { "*" }))
            .collapseAndRemoveUnsupportedPrivileges("*", TransportVersion.current());
        // current version (after the version) has the permission
        assertThat(Set.of(after), equalTo(Set.of(permission)));
    }

    public void testValidate() {
        generateRandomGroups(randomBoolean());
        // random values not allowed
        IllegalArgumentException error = expectThrows(IllegalArgumentException.class, () -> remoteClusterPermission.validate());
        assertTrue(error.getMessage().contains("Invalid remote_cluster permissions found. Please remove the following:"));
        assertTrue(error.getMessage().contains("Only [monitor_enrich, monitor_stats] are allowed"));

        new RemoteClusterPermissions().addGroup(new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "*" }))
            .validate(); // no error
    }

    public void testToMap() {
        RemoteClusterPermissions remoteClusterPermissions = new RemoteClusterPermissions();
        List<RemoteClusterPermissionGroup> groups = generateRandomGroups(randomBoolean());
        for (int i = 0; i < groups.size(); i++) {
            remoteClusterPermissions.addGroup(groups.get(i));
        }
        List<Map<String, List<String>>> asAsMap = remoteClusterPermissions.toMap();
        RemoteClusterPermissions remoteClusterPermissionsAsMap = new RemoteClusterPermissions(asAsMap);
        assertEquals(remoteClusterPermissions, remoteClusterPermissionsAsMap);
    }

    public void testRemoveUnsupportedPrivileges() {
        RemoteClusterPermissions remoteClusterPermissions = new RemoteClusterPermissions();
        RemoteClusterPermissionGroup group = new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "*" });
        remoteClusterPermissions.addGroup(group);
        // this privilege is allowed by versions, so nothing should be removed
        assertEquals(remoteClusterPermissions, remoteClusterPermissions.removeUnsupportedPrivileges(ROLE_REMOTE_CLUSTER_PRIVS));
        assertEquals(remoteClusterPermissions, remoteClusterPermissions.removeUnsupportedPrivileges(ROLE_MONITOR_STATS));

        remoteClusterPermissions = new RemoteClusterPermissions();
        if (randomBoolean()) {
            group = new RemoteClusterPermissionGroup(new String[] { "monitor_stats" }, new String[] { "*" });
        } else {
            // if somehow duplicates end up here, they should not influence removal
            group = new RemoteClusterPermissionGroup(new String[] { "monitor_stats", "monitor_stats" }, new String[] { "*" });
        }
        remoteClusterPermissions.addGroup(group);
        // this single newer privilege is not allowed in the older version, so it should result in an object with no groups
        assertNotEquals(remoteClusterPermissions, remoteClusterPermissions.removeUnsupportedPrivileges(ROLE_REMOTE_CLUSTER_PRIVS));
        assertFalse(remoteClusterPermissions.removeUnsupportedPrivileges(ROLE_REMOTE_CLUSTER_PRIVS).hasAnyPrivileges());
        assertEquals(remoteClusterPermissions, remoteClusterPermissions.removeUnsupportedPrivileges(ROLE_MONITOR_STATS));

        int groupCount = randomIntBetween(1, 5);
        remoteClusterPermissions = new RemoteClusterPermissions();
        group = new RemoteClusterPermissionGroup(new String[] { "monitor_enrich", "monitor_stats" }, new String[] { "*" });
        for (int i = 0; i < groupCount; i++) {
            remoteClusterPermissions.addGroup(group);
        }
        // one of the newer privilege is not allowed in the older version, so it should result in a group with only the allowed privilege
        RemoteClusterPermissions expected = new RemoteClusterPermissions();
        for (int i = 0; i < groupCount; i++) {
            expected.addGroup(new RemoteClusterPermissionGroup(new String[] { "monitor_enrich" }, new String[] { "*" }));
        }
        assertEquals(expected, remoteClusterPermissions.removeUnsupportedPrivileges(ROLE_REMOTE_CLUSTER_PRIVS));
        // both privileges allowed in the newer version, so it should not change the permission
        assertEquals(remoteClusterPermissions, remoteClusterPermissions.removeUnsupportedPrivileges(ROLE_MONITOR_STATS));
    }

    public void testShortCircuitRemoveUnsupportedPrivileges() {
        RemoteClusterPermissions remoteClusterPermissions = new RemoteClusterPermissions();
        assertSame(remoteClusterPermissions, remoteClusterPermissions.removeUnsupportedPrivileges(TransportVersion.current()));
        assertSame(remoteClusterPermissions, remoteClusterPermissions.removeUnsupportedPrivileges(lastTransportVersionPermission));
        assertNotSame(
            remoteClusterPermissions,
            remoteClusterPermissions.removeUnsupportedPrivileges(TransportVersionUtils.getPreviousVersion(lastTransportVersionPermission))
        );
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
        Set<String> all = RemoteClusterPermissions.allowedRemoteClusterPermissions.values()
            .stream()
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
        List<String> randomPermission = randomList(1, all.size(), () -> randomFrom(all));
        return new RemoteClusterPermissions().addGroup(
            new RemoteClusterPermissionGroup(randomPermission.toArray(new String[0]), new String[] { "*" })
        );
    }

    @Override
    protected RemoteClusterPermissions mutateInstance(RemoteClusterPermissions instance) throws IOException {
        return new RemoteClusterPermissions().addGroup(
            new RemoteClusterPermissionGroup(new String[] { "monitor_enrich", "monitor_stats" }, new String[] { "*" })
        ).addGroup(new RemoteClusterPermissionGroup(new String[] { "foobar" }, new String[] { "*" }));
    }

    @Override
    protected RemoteClusterPermissions doParseInstance(XContentParser parser) throws IOException {
        // fromXContent/object parsing isn't supported since we still do old school manual parsing of the role descriptor
        // so this test is silly because it only tests we know how to manually parse the test instance in this test
        // this is needed since we want the other parts from the AbstractXContentSerializingTestCase suite
        RemoteClusterPermissions remoteClusterPermissions = new RemoteClusterPermissions();
        String[] privileges = null;
        String[] clusters = null;
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.START_OBJECT) {
                continue;
            }
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (RoleDescriptor.Fields.PRIVILEGES.match(currentFieldName, parser.getDeprecationHandler())) {
                privileges = XContentUtils.readStringArray(parser, false);

            } else if (RoleDescriptor.Fields.CLUSTERS.match(currentFieldName, parser.getDeprecationHandler())) {
                clusters = XContentUtils.readStringArray(parser, false);
            }
        }
        remoteClusterPermissions.addGroup(new RemoteClusterPermissionGroup(privileges, clusters));
        return remoteClusterPermissions;
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
