/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.user;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestGetUserPrivilegesActionTests extends ESTestCase {

    public void testSecurityDisabled() throws Exception {
        final Settings securityDisabledSettings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.getOperationMode()).thenReturn(License.OperationMode.BASIC);
        final RestGetUserPrivilegesAction action = new RestGetUserPrivilegesAction(
            securityDisabledSettings,
            mock(SecurityContext.class),
            licenseState
        );
        final FakeRestRequest request = new FakeRestRequest();
        final FakeRestChannel channel = new FakeRestChannel(request, true, 1);
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName())) {
            action.handleRequest(request, channel, nodeClient);
        }
        assertThat(channel.capturedResponse(), notNullValue());
        assertThat(channel.capturedResponse().status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(
            channel.capturedResponse().content().utf8ToString(),
            containsString("Security is not enabled but a security rest handler is registered")
        );
    }

    public void testBuildResponse() throws Exception {
        final RestGetUserPrivilegesAction.RestListener listener = new RestGetUserPrivilegesAction.RestListener(null);
        final Set<String> cluster = new LinkedHashSet<>(Arrays.asList("monitor", "manage_ml", "manage_watcher"));
        final Set<ConfigurableClusterPrivilege> conditionalCluster = new LinkedHashSet<>(
            Arrays.asList(
                new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(new LinkedHashSet<>(Arrays.asList("app*"))),
                new ConfigurableClusterPrivileges.ManageApplicationPrivileges(new LinkedHashSet<>(Arrays.asList("app01", "app02")))
            )
        );
        final Set<GetUserPrivilegesResponse.Indices> index = new LinkedHashSet<>(
            Arrays.asList(
                new GetUserPrivilegesResponse.Indices(
                    Arrays.asList("index-1", "index-2", "index-3-*"),
                    Arrays.asList("read", "write"),
                    new LinkedHashSet<>(
                        Arrays.asList(
                            new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "public.*" }, new String[0]),
                            new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "*" }, new String[] { "private.*" })
                        )
                    ),
                    new LinkedHashSet<>(
                        Arrays.asList(
                            new BytesArray("{ \"term\": { \"access\": \"public\" } }"),
                            new BytesArray("{ \"term\": { \"access\": \"standard\" } }")
                        )
                    ),
                    false
                ),
                new GetUserPrivilegesResponse.Indices(
                    Arrays.asList("index-4"),
                    Collections.singleton("all"),
                    Collections.emptySet(),
                    Collections.emptySet(),
                    true
                )
            )
        );
        final Set<GetUserPrivilegesResponse.RemoteIndices> remoteIndex = randomBoolean()
            ? Set.of()
            : new LinkedHashSet<>(
                Arrays.asList(
                    new GetUserPrivilegesResponse.RemoteIndices(
                        new GetUserPrivilegesResponse.Indices(
                            Arrays.asList("remote-index-1", "remote-index-2", "remote-index-3-*"),
                            List.of("read"),
                            new LinkedHashSet<>(
                                Arrays.asList(
                                    new FieldPermissionsDefinition.FieldGrantExcludeGroup(new String[] { "public.*" }, new String[0]),
                                    new FieldPermissionsDefinition.FieldGrantExcludeGroup(
                                        new String[] { "*" },
                                        new String[] { "private.*" }
                                    )
                                )
                            ),
                            new LinkedHashSet<>(
                                List.of(
                                    new BytesArray("{ \"term\": { \"access\": \"public\" } }"),
                                    new BytesArray("{ \"term\": { \"access\": \"standard\" } }")
                                )
                            ),
                            false
                        ),
                        new LinkedHashSet<>(List.of("remote-*"))
                    ),
                    new GetUserPrivilegesResponse.RemoteIndices(
                        new GetUserPrivilegesResponse.Indices(
                            List.of("remote-index-4"),
                            Collections.singleton("all"),
                            Collections.emptySet(),
                            Collections.emptySet(),
                            true
                        ),
                        new LinkedHashSet<>(Arrays.asList("*", "remote-2"))
                    )
                )
            );
        final Set<ApplicationResourcePrivileges> application = Sets.newHashSet(
            ApplicationResourcePrivileges.builder().application("app01").privileges("read", "write").resources("*").build(),
            ApplicationResourcePrivileges.builder().application("app01").privileges("admin").resources("department/1").build(),
            ApplicationResourcePrivileges.builder().application("app02").privileges("all").resources("tenant/42", "tenant/99").build()
        );
        final Set<String> runAs = new LinkedHashSet<>(Arrays.asList("app-user-*", "backup-user"));
        final GetUserPrivilegesResponse response = new GetUserPrivilegesResponse(
            cluster,
            conditionalCluster,
            index,
            application,
            runAs,
            remoteIndex
        );
        XContentBuilder builder = jsonBuilder();
        listener.buildResponse(response, builder);

        String json = Strings.toString(builder);
        String remoteIndicesSection = remoteIndex.isEmpty() ? "" : """
            , "remote_indices": [
                {
                  "names": [ "remote-index-1", "remote-index-2", "remote-index-3-*" ],
                  "privileges": [ "read" ],
                  "field_security": [
                    {
                      "grant": [ "*" ],
                      "except": [ "private.*" ]
                    },
                    {
                      "grant": [ "public.*" ]
                    }
                  ],
                  "query": [ "{ \\"term\\": { \\"access\\": \\"public\\" } }", "{ \\"term\\": { \\"access\\": \\"standard\\" } }" ],
                  "allow_restricted_indices": false,
                  "clusters": [ "remote-*" ]
                },
                {
                  "names": [ "remote-index-4" ],
                  "privileges": [ "all" ],
                  "allow_restricted_indices": true,
                  "clusters": [ "*", "remote-2" ]
                }
              ]""";
        assertThat(json, equalTo(XContentHelper.stripWhitespace(Strings.format("""
            {
              "cluster": [ "monitor", "manage_ml", "manage_watcher" ],
              "global": [
                {
                  "profile": {
                    "write": {
                      "applications": [ "app*" ]
                    }
                  }
                },
                {
                  "application": {
                    "manage": {
                      "applications": [ "app01", "app02" ]
                    }
                  }
                }
              ],
              "indices": [
                {
                  "names": [ "index-1", "index-2", "index-3-*" ],
                  "privileges": [ "read", "write" ],
                  "field_security": [
                    {
                      "grant": [ "*" ],
                      "except": [ "private.*" ]
                    },
                    {
                      "grant": [ "public.*" ]
                    }
                  ],
                  "query": [ "{ \\"term\\": { \\"access\\": \\"public\\" } }", "{ \\"term\\": { \\"access\\": \\"standard\\" } }" ],
                  "allow_restricted_indices": false
                },
                {
                  "names": [ "index-4" ],
                  "privileges": [ "all" ],
                  "allow_restricted_indices": true
                }
              ],
              "applications": [
                {
                  "application": "app01",
                  "privileges": [ "read", "write" ],
                  "resources": [ "*" ]
                },
                {
                  "application": "app01",
                  "privileges": [ "admin" ],
                  "resources": [ "department/1" ]
                },
                {
                  "application": "app02",
                  "privileges": [ "all" ],
                  "resources": [ "tenant/42", "tenant/99" ]
                }
              ],
              "run_as": [ "app-user-*", "backup-user" ]%s
            }""", remoteIndicesSection))));
    }
}
