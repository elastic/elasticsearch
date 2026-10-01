/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.StringMatcher;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.RESTRICTED_INDICES;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IndicesPermissionTests extends ESTestCase {

    public void testAuthorize() {
        IndexMetadata.Builder imbBuilder = IndexMetadata.builder("_index")
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .putAlias(AliasMetadata.builder("_alias"));
        ProjectMetadata pmd = ProjectMetadata.builder(randomProjectIdOrDefault()).put(imbBuilder.build(), true).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);

        // basics:
        Set<BytesReference> query = Collections.singleton(new BytesArray("{}"));
        String[] fields = new String[] { "_field" };
        Role role = Role.builder(RESTRICTED_INDICES, "_role")
            .add(new FieldPermissions(fieldPermissionDef(fields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_index")
            .build();
        IndicesAccessControl permissions = role.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet("_index"),
            pmd,
            fieldPermissionsCache
        );
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), hasSize(1));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), equalTo(query));

        // no document level security:
        role = Role.builder(RESTRICTED_INDICES, "_role")
            .add(new FieldPermissions(fieldPermissionDef(fields, null)), null, IndexPrivilege.ALL, randomBoolean(), "_index")
            .build();
        permissions = role.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("_index"), pmd, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(false));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getListOfQueries(), nullValue());

        // no field level security:
        role = Role.builder(RESTRICTED_INDICES, "_role")
            .add(FieldPermissions.DEFAULT, query, IndexPrivilege.ALL, randomBoolean(), "_index")
            .build();
        permissions = role.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("_index"), pmd, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), hasSize(1));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), equalTo(query));

        // index group associated with an alias:
        role = Role.builder(RESTRICTED_INDICES, "_role")
            .add(new FieldPermissions(fieldPermissionDef(fields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_alias")
            .build();
        permissions = role.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("_alias"), pmd, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), hasSize(1));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), equalTo(query));

        assertThat(permissions.getIndexPermissions("_alias"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_alias").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_alias").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().getSingleSetOfQueries(), hasSize(1));
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().getSingleSetOfQueries(), equalTo(query));

        // match all fields
        String[] allFields = randomFrom(
            new String[] { "*" },
            new String[] { "foo", "*" },
            new String[] { randomAlphaOfLengthBetween(1, 10), "*" }
        );
        role = Role.builder(RESTRICTED_INDICES, "_role")
            .add(new FieldPermissions(fieldPermissionDef(allFields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_alias")
            .build();
        permissions = role.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("_alias"), pmd, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), hasSize(1));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), equalTo(query));

        assertThat(permissions.getIndexPermissions("_alias"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_alias").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().getSingleSetOfQueries(), hasSize(1));
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().getSingleSetOfQueries(), equalTo(query));

        IndexMetadata.Builder imbBuilder1 = IndexMetadata.builder("_index_1")
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .putAlias(AliasMetadata.builder("_alias"));
        pmd = ProjectMetadata.builder(pmd).put(imbBuilder1.build(), true).build();

        // match all fields with more than one permission
        Set<BytesReference> fooQuery = Collections.singleton(new BytesArray("{foo}"));
        allFields = randomFrom(new String[] { "*" }, new String[] { "foo", "*" }, new String[] { randomAlphaOfLengthBetween(1, 10), "*" });
        role = Role.builder(RESTRICTED_INDICES, "_role")
            .add(new FieldPermissions(fieldPermissionDef(allFields, null)), fooQuery, IndexPrivilege.ALL, randomBoolean(), "_alias")
            .add(new FieldPermissions(fieldPermissionDef(allFields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_alias")
            .build();
        permissions = role.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("_alias"), pmd, fieldPermissionsCache);
        Set<BytesReference> bothQueries = Sets.union(fooQuery, query);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), hasSize(2));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), equalTo(bothQueries));

        assertThat(permissions.getIndexPermissions("_index_1"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index_1").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index_1").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index_1").getDocumentPermissions().getSingleSetOfQueries(), hasSize(2));
        assertThat(permissions.getIndexPermissions("_index_1").getDocumentPermissions().getSingleSetOfQueries(), equalTo(bothQueries));

        assertThat(permissions.getIndexPermissions("_alias"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_alias").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().getSingleSetOfQueries(), hasSize(2));
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().getSingleSetOfQueries(), equalTo(bothQueries));

    }

    public void testAuthorizeDataStreamAccessWithFailuresSelector() {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        String dataStreamName = randomAlphaOfLength(6);
        int numBackingIndices = randomIntBetween(1, 3);
        List<IndexMetadata> backingIndices = new ArrayList<>();
        for (int backingIndexNumber = 1; backingIndexNumber <= numBackingIndices; backingIndexNumber++) {
            backingIndices.add(createBackingIndexMetadata(DataStream.getDefaultBackingIndexName(dataStreamName, backingIndexNumber)));
        }
        DataStream ds = DataStreamTestHelper.newInstance(
            dataStreamName,
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList())
        );
        builder.put(ds);
        for (IndexMetadata index : backingIndices) {
            builder.put(index, false);
        }
        var metadata = builder.build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);

        for (var privilege : List.of(IndexPrivilege.ALL, IndexPrivilege.READ)) {
            Role role = Role.builder(RESTRICTED_INDICES, "_role")
                .add(
                    new FieldPermissions(fieldPermissionDef(null, null)),
                    null,
                    privilege,
                    randomBoolean(),
                    randomFrom(dataStreamName, dataStreamName + "*")
                )
                .build();
            IndicesAccessControl permissions = role.authorize(
                TransportSearchAction.TYPE.name(),
                Sets.newHashSet(randomFrom(dataStreamName, dataStreamName + "::data")),
                metadata,
                fieldPermissionsCache
            );
            assertThat("for privilege " + privilege, permissions.isGranted(), is(true));
            assertThat("for privilege " + privilege, permissions.hasIndexPermissions(dataStreamName + "::failures"), is(false));
            assertThat("for privilege " + privilege, permissions.hasIndexPermissions(dataStreamName), is(true));
        }

        for (var privilege : List.of(IndexPrivilege.ALL, IndexPrivilege.READ_FAILURE_STORE)) {
            Role role = Role.builder(RESTRICTED_INDICES, "_role")
                .add(
                    new FieldPermissions(fieldPermissionDef(null, null)),
                    null,
                    privilege,
                    randomBoolean(),
                    randomFrom(dataStreamName, dataStreamName + "*")
                )
                .build();
            IndicesAccessControl permissions = role.authorize(
                TransportSearchAction.TYPE.name(),
                Sets.newHashSet(dataStreamName + "::failures"),
                metadata,
                fieldPermissionsCache
            );
            assertThat("for privilege " + privilege, permissions.isGranted(), is(true));
            assertThat("for privilege " + privilege, permissions.hasIndexPermissions(dataStreamName + "::failures"), is(true));
            assertThat("for privilege " + privilege, permissions.hasIndexPermissions(dataStreamName), is(false));
        }

        for (var privilege : List.of(IndexPrivilege.ALL, IndexPrivilege.READ_FAILURE_STORE)) {
            Role role = Role.builder(RESTRICTED_INDICES, "_role")
                .add(
                    new FieldPermissions(fieldPermissionDef(null, null)),
                    null,
                    privilege,
                    randomBoolean(),
                    randomFrom(dataStreamName, dataStreamName + "*")
                )
                .build();
            IndicesAccessControl permissions = role.authorize(
                TransportSearchAction.TYPE.name(),
                Sets.newHashSet(dataStreamName + "::failures"),
                metadata,
                fieldPermissionsCache
            );

            assertThat("for privilege " + privilege, permissions.isGranted(), is(true));
            assertThat("for privilege " + privilege, permissions.hasIndexPermissions(dataStreamName + "::failures"), is(true));
            assertThat("for privilege " + privilege, permissions.hasIndexPermissions(dataStreamName), is(false));
        }

        {
            Role role = Role.builder(RESTRICTED_INDICES, "_role")
                .add(
                    new FieldPermissions(fieldPermissionDef(null, null)),
                    null,
                    IndexPrivilege.READ,
                    randomBoolean(),
                    randomFrom(dataStreamName, dataStreamName + "*")
                )
                .add(
                    new FieldPermissions(fieldPermissionDef(null, null)),
                    null,
                    IndexPrivilege.READ_FAILURE_STORE,
                    randomBoolean(),
                    randomFrom(dataStreamName, dataStreamName + "*")
                )
                .build();
            IndicesAccessControl permissions = role.authorize(
                TransportSearchAction.TYPE.name(),
                Sets.newHashSet(randomFrom(dataStreamName, dataStreamName + "::data"), dataStreamName + "::failures"),
                metadata,
                fieldPermissionsCache
            );
            assertThat(permissions.isGranted(), is(true));
            assertThat(permissions.hasIndexPermissions(dataStreamName + "::failures"), is(true));
            assertThat(permissions.hasIndexPermissions(dataStreamName), is(true));
        }

        {
            Role role = Role.builder(RESTRICTED_INDICES, "_role")
                .add(
                    new FieldPermissions(fieldPermissionDef(null, null)),
                    null,
                    IndexPrivilege.ALL,
                    randomBoolean(),
                    randomFrom(dataStreamName, dataStreamName + "*")
                )
                .build();
            IndicesAccessControl permissions = role.authorize(
                TransportSearchAction.TYPE.name(),
                Sets.newHashSet(randomFrom(dataStreamName, dataStreamName + "::data"), dataStreamName + "::failures"),
                metadata,
                fieldPermissionsCache
            );
            assertThat("for privilege " + IndexPrivilege.ALL, permissions.isGranted(), is(true));
            assertThat("for privilege " + IndexPrivilege.ALL, permissions.hasIndexPermissions(dataStreamName + "::failures"), is(true));
            assertThat("for privilege " + IndexPrivilege.ALL, permissions.hasIndexPermissions(dataStreamName), is(true));
        }
        {
            Role role = Role.builder(RESTRICTED_INDICES, "_role")
                .add(
                    new FieldPermissions(fieldPermissionDef(null, null)),
                    null,
                    IndexPrivilege.READ_FAILURE_STORE,
                    randomBoolean(),
                    randomFrom(dataStreamName, dataStreamName + "*")
                )
                .build();
            IndicesAccessControl permissions = role.authorize(
                TransportSearchAction.TYPE.name(),
                Sets.newHashSet(randomFrom(dataStreamName, dataStreamName + "::data"), dataStreamName + "::failures"),
                metadata,
                fieldPermissionsCache
            );
            assertThat("for privilege " + IndexPrivilege.READ_FAILURE_STORE, permissions.isGranted(), is(false));
            assertThat(
                "for privilege " + IndexPrivilege.READ_FAILURE_STORE,
                permissions.hasIndexPermissions(dataStreamName + "::failures"),
                is(true)
            );
            assertThat("for privilege " + IndexPrivilege.READ_FAILURE_STORE, permissions.hasIndexPermissions(dataStreamName), is(false));
        }

        {
            Role role = Role.builder(RESTRICTED_INDICES, "_role")
                .add(
                    new FieldPermissions(fieldPermissionDef(null, null)),
                    null,
                    IndexPrivilege.READ,
                    randomBoolean(),
                    randomFrom(dataStreamName, dataStreamName + "*")
                )
                .build();
            IndicesAccessControl permissions = role.authorize(
                TransportSearchAction.TYPE.name(),
                Sets.newHashSet(randomFrom(dataStreamName, dataStreamName + "::data"), dataStreamName + "::failures"),
                metadata,
                fieldPermissionsCache
            );
            assertThat("for privilege " + IndexPrivilege.READ, permissions.isGranted(), is(false));
            assertThat("for privilege " + IndexPrivilege.READ, permissions.hasIndexPermissions(dataStreamName + "::failures"), is(false));
            assertThat("for privilege " + IndexPrivilege.READ, permissions.hasIndexPermissions(dataStreamName), is(true));
        }
    }

    public void testAuthorizeDataStreamFailureIndices() {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        String dataStreamName = randomAlphaOfLength(6);
        int numBackingIndices = randomIntBetween(1, 3);
        List<IndexMetadata> backingIndices = new ArrayList<>();
        for (int backingIndexNumber = 1; backingIndexNumber <= numBackingIndices; backingIndexNumber++) {
            backingIndices.add(createBackingIndexMetadata(DataStream.getDefaultBackingIndexName(dataStreamName, backingIndexNumber)));
        }
        List<IndexMetadata> failureIndices = new ArrayList<>();
        int numFailureIndices = randomIntBetween(1, 3);
        for (int failureIndexNumber = 1; failureIndexNumber <= numFailureIndices; failureIndexNumber++) {
            failureIndices.add(createBackingIndexMetadata(DataStream.getDefaultFailureStoreName(dataStreamName, failureIndexNumber, 1L)));
        }
        DataStream ds = DataStreamTestHelper.newInstance(
            dataStreamName,
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()),
            failureIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList())
        );
        builder.put(ds);
        for (IndexMetadata index : backingIndices) {
            builder.put(index, false);
        }
        for (IndexMetadata index : failureIndices) {
            builder.put(index, false);
        }
        var metadata = builder.build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);

        for (var privilege : List.of(IndexPrivilege.READ)) {
            Role role = Role.builder(RESTRICTED_INDICES, "_role")
                .add(
                    new FieldPermissions(fieldPermissionDef(null, null)),
                    null,
                    privilege,
                    randomBoolean(),
                    randomFrom(dataStreamName, dataStreamName + "*")
                )
                .build();
            String failureIndex = randomFrom(failureIndices).getIndex().getName();
            IndicesAccessControl permissions = role.authorize(
                TransportSearchAction.TYPE.name(),
                Sets.newHashSet(failureIndex),
                metadata,
                fieldPermissionsCache
            );
            assertThat("for privilege " + privilege, permissions.isGranted(), is(false));
            assertThat("for privilege " + privilege, permissions.hasIndexPermissions(failureIndex), is(false));

            String dataIndex = randomFrom(backingIndices).getIndex().getName();
            permissions = role.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet(dataIndex), metadata, fieldPermissionsCache);

            assertThat("for privilege " + privilege, permissions.isGranted(), is(true));
            assertThat("for privilege " + privilege, permissions.hasIndexPermissions(dataIndex), is(true));
        }

        for (var privilege : List.of(IndexPrivilege.READ_FAILURE_STORE)) {
            Role role = Role.builder(RESTRICTED_INDICES, "_role")
                .add(
                    new FieldPermissions(fieldPermissionDef(null, null)),
                    null,
                    privilege,
                    randomBoolean(),
                    randomFrom(dataStreamName, dataStreamName + "*")
                )
                .build();

            String failureIndex = randomFrom(failureIndices).getIndex().getName();
            IndicesAccessControl permissions = role.authorize(
                TransportSearchAction.TYPE.name(),
                Sets.newHashSet(failureIndex),
                metadata,
                fieldPermissionsCache
            );

            assertThat("for privilege " + privilege, permissions.isGranted(), is(true));
            assertThat("for privilege " + privilege, permissions.hasIndexPermissions(failureIndex), is(true));

            String dataIndex = randomFrom(backingIndices).getIndex().getName();
            permissions = role.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet(dataIndex), metadata, fieldPermissionsCache);

            assertThat("for privilege " + privilege, permissions.isGranted(), is(false));
            assertThat("for privilege " + privilege, permissions.hasIndexPermissions(dataIndex), is(false));
        }
    }

    public void testAuthorizeMultipleGroupsMixedDls() {
        IndexMetadata.Builder imbBuilder = IndexMetadata.builder("_index")
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .putAlias(AliasMetadata.builder("_alias"));
        ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(imbBuilder.build(), true).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);

        Set<BytesReference> query = Collections.singleton(new BytesArray("{}"));
        String[] fields = new String[] { "_field" };
        Role role = Role.builder(RESTRICTED_INDICES, "_role")
            .add(new FieldPermissions(fieldPermissionDef(fields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_index")
            .add(new FieldPermissions(fieldPermissionDef(null, null)), null, IndexPrivilege.ALL, randomBoolean(), "*")
            .build();
        IndicesAccessControl permissions = role.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet("_index"),
            projectMetadata,
            fieldPermissionsCache
        );
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().grantsAccessTo("_field"));
        assertFalse(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertFalse(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions());
    }

    public void testIndicesPrivilegesStreaming() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        String[] allowed = new String[] { randomAlphaOfLength(5) + "*", randomAlphaOfLength(5) + "*", randomAlphaOfLength(5) + "*" };
        String[] denied = new String[] {
            allowed[0] + randomAlphaOfLength(5),
            allowed[1] + randomAlphaOfLength(5),
            allowed[2] + randomAlphaOfLength(5) };
        RoleDescriptor.IndicesPrivileges.Builder indicesPrivileges = RoleDescriptor.IndicesPrivileges.builder();
        indicesPrivileges.grantedFields(allowed);
        indicesPrivileges.deniedFields(denied);
        indicesPrivileges.query("{match_all:{}}");
        indicesPrivileges.indices(randomAlphaOfLength(5), randomAlphaOfLength(5), randomAlphaOfLength(5));
        indicesPrivileges.privileges("all", "read", "priv");
        indicesPrivileges.build().writeTo(out);
        out.close();
        StreamInput in = out.bytes().streamInput();
        RoleDescriptor.IndicesPrivileges readIndicesPrivileges = new RoleDescriptor.IndicesPrivileges(in);
        assertEquals(readIndicesPrivileges, indicesPrivileges.build());

        out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersion.current());
        indicesPrivileges = RoleDescriptor.IndicesPrivileges.builder();
        indicesPrivileges.grantedFields(allowed);
        indicesPrivileges.deniedFields(denied);
        indicesPrivileges.query("{match_all:{}}");
        indicesPrivileges.indices(readIndicesPrivileges.getIndices());
        indicesPrivileges.privileges("all", "read", "priv");
        indicesPrivileges.build().writeTo(out);
        out.close();
        in = out.bytes().streamInput();
        in.setTransportVersion(TransportVersion.current());
        RoleDescriptor.IndicesPrivileges readIndicesPrivileges2 = new RoleDescriptor.IndicesPrivileges(in);
        assertEquals(readIndicesPrivileges, readIndicesPrivileges2);
    }

    // tests that field permissions are merged correctly when we authorize with several groups and don't crash when an index has no group
    public void testCorePermissionAuthorize() {
        final Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        final var metadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .build();

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        IndicesPermission core = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            null,
            randomBoolean(),
            "a1"
        )
            .addGroup(
                IndexPrivilege.READ,
                new FieldPermissions(fieldPermissionDef(null, new String[] { "denied_field" })),
                null,
                randomBoolean(),
                "a1"
            )
            .build();
        IndicesAccessControl iac = core.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet("a1", "ba"),
            metadata,
            fieldPermissionsCache
        );
        assertTrue(iac.getIndexPermissions("a1").getFieldPermissions().grantsAccessTo("denied_field"));
        assertTrue(iac.getIndexPermissions("a1").getFieldPermissions().grantsAccessTo(randomAlphaOfLength(5)));
        // did not define anything for ba so we allow all
        assertFalse(iac.hasIndexPermissions("ba"));

        assertTrue(core.check(TransportSearchAction.TYPE.name()));
        assertTrue(core.check(TransportPutMappingAction.TYPE.name()));
        assertTrue(core.check(TransportAutoPutMappingAction.TYPE.name()));
        assertFalse(core.check("unknown"));

        // test with two indices
        core = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            null,
            randomBoolean(),
            "a1"
        )
            .addGroup(
                IndexPrivilege.ALL,
                new FieldPermissions(fieldPermissionDef(null, new String[] { "denied_field" })),
                null,
                randomBoolean(),
                "a1"
            )
            .addGroup(
                IndexPrivilege.ALL,
                new FieldPermissions(fieldPermissionDef(new String[] { "*_field" }, new String[] { "denied_field" })),
                null,
                randomBoolean(),
                "a2"
            )
            .addGroup(
                IndexPrivilege.ALL,
                new FieldPermissions(fieldPermissionDef(new String[] { "*_field2" }, new String[] { "denied_field2" })),
                null,
                randomBoolean(),
                "a2"
            )
            .build();
        iac = core.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("a1", "a2"), metadata, fieldPermissionsCache);
        assertFalse(iac.getIndexPermissions("a1").getFieldPermissions().hasFieldLevelSecurity());
        assertFalse(iac.getIndexPermissions("a2").getFieldPermissions().grantsAccessTo("denied_field2"));
        assertFalse(iac.getIndexPermissions("a2").getFieldPermissions().grantsAccessTo("denied_field"));
        assertTrue(iac.getIndexPermissions("a2").getFieldPermissions().grantsAccessTo(randomAlphaOfLength(5) + "_field"));
        assertTrue(iac.getIndexPermissions("a2").getFieldPermissions().grantsAccessTo(randomAlphaOfLength(5) + "_field2"));
        assertTrue(iac.getIndexPermissions("a2").getFieldPermissions().hasFieldLevelSecurity());

        assertTrue(core.check(TransportSearchAction.TYPE.name()));
        assertTrue(core.check(TransportPutMappingAction.TYPE.name()));
        assertTrue(core.check(TransportAutoPutMappingAction.TYPE.name()));
        assertFalse(core.check("unknown"));
    }

    public void testErrorMessageIfIndexPatternIsTooComplex() {
        List<String> indices = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String prefix = randomAlphaOfLengthBetween(4, 12);
            String suffixBegin = randomAlphaOfLengthBetween(12, 36);
            indices.add("*" + prefix + "*" + suffixBegin + "*");
        }
        final ElasticsearchSecurityException e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> new IndicesPermission.Group(
                IndexPrivilege.ALL,
                FieldPermissions.DEFAULT,
                null,
                randomBoolean(),
                RESTRICTED_INDICES,
                false,
                indices.toArray(Strings.EMPTY_ARRAY)
            )
        );
        assertThat(e.getMessage(), containsString(indices.get(0)));
        assertThat(e.getMessage(), containsString("too complex to evaluate"));
    }

    public void testSecurityIndicesPermissions() {
        final Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        final var metadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(
                new IndexMetadata.Builder(internalSecurityIndex).settings(indexSettings)
                    .numberOfShards(1)
                    .numberOfReplicas(0)
                    .putAlias(new AliasMetadata.Builder(SecuritySystemIndices.SECURITY_MAIN_ALIAS).build())
                    .build(),
                true
            )
            .build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);

        // allow_restricted_indices: false
        IndicesPermission indicesPermission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            null,
            false,
            "*"
        ).build();
        IndicesAccessControl iac = indicesPermission.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet(internalSecurityIndex, SecuritySystemIndices.SECURITY_MAIN_ALIAS),
            metadata,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(false));
        assertThat(iac.hasIndexPermissions(internalSecurityIndex), is(false));
        assertThat(iac.getIndexPermissions(internalSecurityIndex), is(nullValue()));
        assertThat(iac.hasIndexPermissions(SecuritySystemIndices.SECURITY_MAIN_ALIAS), is(false));
        assertThat(iac.getIndexPermissions(SecuritySystemIndices.SECURITY_MAIN_ALIAS), is(nullValue()));

        // allow_restricted_indices: true
        indicesPermission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            null,
            true,
            "*"
        ).build();
        iac = indicesPermission.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet(internalSecurityIndex, SecuritySystemIndices.SECURITY_MAIN_ALIAS),
            metadata,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(true));
        assertThat(iac.hasIndexPermissions(internalSecurityIndex), is(true));
        assertThat(iac.getIndexPermissions(internalSecurityIndex), is(notNullValue()));
        assertThat(iac.hasIndexPermissions(SecuritySystemIndices.SECURITY_MAIN_ALIAS), is(true));
        assertThat(iac.getIndexPermissions(SecuritySystemIndices.SECURITY_MAIN_ALIAS), is(notNullValue()));
    }

    public void testAsyncSearchIndicesPermissions() {
        final Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        final String asyncSearchIndex = XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2);
        final var metadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(new IndexMetadata.Builder(asyncSearchIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);

        // allow_restricted_indices: false
        IndicesPermission indicesPermission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            null,
            false,
            "*"
        ).build();
        IndicesAccessControl iac = indicesPermission.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet(asyncSearchIndex),
            metadata,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(false));
        assertThat(iac.hasIndexPermissions(asyncSearchIndex), is(false));
        assertThat(iac.getIndexPermissions(asyncSearchIndex), is(nullValue()));

        // allow_restricted_indices: true
        indicesPermission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            null,
            true,
            "*"
        ).build();
        iac = indicesPermission.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet(asyncSearchIndex),
            metadata,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(true));
        assertThat(iac.hasIndexPermissions(asyncSearchIndex), is(true));
        assertThat(iac.getIndexPermissions(asyncSearchIndex), is(notNullValue()));
    }

    public void testAuthorizationForBackingIndices() {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        String dataStreamName = randomAlphaOfLength(6);
        int numBackingIndices = randomIntBetween(1, 3);
        List<IndexMetadata> backingIndices = new ArrayList<>();
        for (int backingIndexNumber = 1; backingIndexNumber <= numBackingIndices; backingIndexNumber++) {
            backingIndices.add(createBackingIndexMetadata(DataStream.getDefaultBackingIndexName(dataStreamName, backingIndexNumber)));
        }
        DataStream ds = DataStreamTestHelper.newInstance(
            dataStreamName,
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList())
        );
        builder.put(ds);
        for (IndexMetadata index : backingIndices) {
            builder.put(index, false);
        }
        var metadata = builder.build();

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        IndicesPermission indicesPermission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.READ,
            FieldPermissions.DEFAULT,
            null,
            false,
            dataStreamName
        ).build();
        IndicesAccessControl iac = indicesPermission.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList())),
            metadata,
            fieldPermissionsCache
        );

        assertThat(iac.isGranted(), is(true));
        for (IndexMetadata im : backingIndices) {
            assertThat(iac.getIndexPermissions(im.getIndex().getName()), is(notNullValue()));
            assertThat(iac.hasIndexPermissions(im.getIndex().getName()), is(true));
        }

        indicesPermission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.CREATE_DOC,
            FieldPermissions.DEFAULT,
            null,
            false,
            dataStreamName
        ).build();
        iac = indicesPermission.authorize(
            randomFrom(TransportPutMappingAction.TYPE.name(), TransportAutoPutMappingAction.TYPE.name()),
            Sets.newHashSet(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList())),
            metadata,
            fieldPermissionsCache
        );

        assertThat(iac.isGranted(), is(false));
        for (IndexMetadata im : backingIndices) {
            assertThat(iac.getIndexPermissions(im.getIndex().getName()), is(nullValue()));
            assertThat(iac.hasIndexPermissions(im.getIndex().getName()), is(false));
        }
    }

    public void testBackingIndicesShareOneIndexAccessControlInstance() {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        String dataStreamName = randomAlphaOfLength(6);
        int numBackingIndices = randomIntBetween(3, 10);
        List<IndexMetadata> backingIndices = new ArrayList<>();
        for (int backingIndexNumber = 1; backingIndexNumber <= numBackingIndices; backingIndexNumber++) {
            backingIndices.add(createBackingIndexMetadata(DataStream.getDefaultBackingIndexName(dataStreamName, backingIndexNumber)));
        }
        builder.put(
            DataStreamTestHelper.newInstance(
                dataStreamName,
                backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList())
            )
        );
        for (IndexMetadata index : backingIndices) {
            builder.put(index, false);
        }
        ProjectMetadata metadata = builder.build();

        // Explicit DLS and FLS, matching the project that hit the OOM.
        IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.READ,
            new FieldPermissions(fieldPermissionDef(new String[] { "_field" }, null)),
            Collections.singleton(new BytesArray("{}")),
            false,
            dataStreamName
        ).build();

        IndicesAccessControl iac = permission.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet(dataStreamName),
            metadata,
            new FieldPermissionsCache(Settings.EMPTY)
        );

        assertThat(iac.isGranted(), is(true));
        IndicesAccessControl.IndexAccessControl dataStreamAccess = iac.getIndexPermissions(dataStreamName);
        assertThat(dataStreamAccess, is(notNullValue()));
        assertThat(dataStreamAccess.getFieldPermissions().hasFieldLevelSecurity(), is(true));
        assertThat(dataStreamAccess.getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat("explicit DLS/FLS must not be marked implicit", dataStreamAccess.isDlsFlsImplicit(), is(false));

        for (IndexMetadata im : backingIndices) {
            String backingIndexName = im.getIndex().getName();
            assertSame(
                "backing index [" + backingIndexName + "] must share the data stream's IndexAccessControl instance",
                dataStreamAccess,
                iac.getIndexPermissions(backingIndexName)
            );
        }
    }

    /**
     * A concrete index requested with a {@code ::failures} selector resolves to the index itself, so the key it is
     * authorized under ({@code <index>::failures}) is not among its concrete names. Like an alias or data stream key,
     * that key must carry the DLS/FLS of the group that granted it rather than fall back to an unrestricted entry.
     * Covers both a failure index granted through its parent data stream and a plain index granted directly.
     */
    public void testFailuresSelectorOnConcreteIndexKeepsDlsFlsOfGrantingGroup() {
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        String dataStreamName = randomAlphaOfLength(6);
        IndexMetadata backingIndex = createBackingIndexMetadata(DataStream.getDefaultBackingIndexName(dataStreamName, 1));
        IndexMetadata failureIndex = createBackingIndexMetadata(DataStream.getDefaultFailureStoreName(dataStreamName, 1, 1L));
        builder.put(DataStreamTestHelper.newInstance(dataStreamName, List.of(backingIndex.getIndex()), List.of(failureIndex.getIndex())));
        builder.put(backingIndex, false);
        builder.put(failureIndex, false);
        String plainIndexName = randomAlphaOfLength(8);
        builder.put(IndexMetadata.builder(plainIndexName).settings(indexSettings(IndexVersion.current(), 1, 1)).build(), false);
        ProjectMetadata metadata = builder.build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);

        IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.READ_FAILURE_STORE,
            new FieldPermissions(fieldPermissionDef(new String[] { "_field" }, null)),
            Collections.singleton(new BytesArray("{}")),
            false,
            dataStreamName,
            plainIndexName
        ).build();

        for (String requested : List.of(failureIndex.getIndex().getName() + "::failures", plainIndexName + "::failures")) {
            IndicesAccessControl iac = permission.authorize(
                TransportSearchAction.TYPE.name(),
                Sets.newHashSet(requested),
                metadata,
                fieldPermissionsCache
            );
            assertThat("for [" + requested + "]", iac.isGranted(), is(true));
            IndicesAccessControl.IndexAccessControl access = iac.getIndexPermissions(requested);
            assertThat("for [" + requested + "]", access, is(notNullValue()));
            assertThat("for [" + requested + "]", access.getFieldPermissions().hasFieldLevelSecurity(), is(true));
            assertThat("for [" + requested + "]", access.getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
            assertThat("for [" + requested + "]", access.isDlsFlsImplicit(), is(false));
        }
    }

    /**
     * A data stream grant with DLS and an unrestricted grant on one backing index (different name sets, so they are
     * not merged at role-build time). The backing index is relaxed only when it is itself requested: then its entry
     * is the union of both grants (allow-all documents) while every other backing index, and the data stream entry,
     * keep the data stream's DLS and keep sharing one {@code IndexAccessControl}. When only the data stream is
     * requested the direct grant does not apply and all backing indices share the restricted entry.
     *
     * <p>The data stream entry assertion is the regression test for the pre-refactoring behaviour, where that entry
     * was written by reference from the last backing index and could become allow-all depending on the iteration
     * order of the requested names. The scenario therefore runs with names covering both orders.
     */
    public void testDirectlyRequestedBackingIndexWithUnrestrictedGrantIsRelaxedWithoutAffectingDataStreamEntry() {
        final Set<BytesReference> query = Collections.singleton(new BytesArray("{\"term\":{\"tenant\":\"a\"}}"));
        final int numBackingIndices = randomIntBetween(3, 6);
        for (String dataStreamName : dataStreamNamesCoveringBothIterationOrders(numBackingIndices)) {
            final ProjectMetadata metadata = dataStreamProjectMetadata(dataStreamName, numBackingIndices, false);
            final List<String> backingIndices = backingIndexNames(metadata, dataStreamName);
            final String writeIndex = backingIndices.get(backingIndices.size() - 1);
            final IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
                IndexPrivilege.READ,
                FieldPermissions.DEFAULT,
                query,
                false,
                dataStreamName
            ).addGroup(IndexPrivilege.READ, FieldPermissions.DEFAULT, null, false, writeIndex).build();
            final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);

            // data stream and its write index requested together
            IndicesAccessControl iac = permission.authorize(
                TransportSearchAction.TYPE.name(),
                Sets.newHashSet(dataStreamName, writeIndex),
                metadata,
                fieldPermissionsCache
            );
            assertThat(iac.isGranted(), is(true));
            final IndicesAccessControl.IndexAccessControl dataStreamAccess = iac.getIndexPermissions(dataStreamName);
            assertThat(dataStreamAccess, is(notNullValue()));
            assertThat(dataStreamAccess.getDocumentPermissions().getSingleSetOfQueries(), equalTo(query));
            assertThat(dataStreamAccess.isDlsFlsImplicit(), is(false));
            final IndicesAccessControl.IndexAccessControl writeIndexAccess = iac.getIndexPermissions(writeIndex);
            assertThat(writeIndexAccess, is(notNullValue()));
            assertThat(writeIndexAccess.getDocumentPermissions().hasDocumentLevelPermissions(), is(false));
            assertThat(writeIndexAccess.getFieldPermissions().hasFieldLevelSecurity(), is(false));
            assertThat(writeIndexAccess.isDlsFlsImplicit(), is(false));
            assertNotSame(dataStreamAccess, writeIndexAccess);
            for (String backingIndex : backingIndices.subList(0, backingIndices.size() - 1)) {
                assertSame("[" + backingIndex + "] for [" + dataStreamName + "]", dataStreamAccess, iac.getIndexPermissions(backingIndex));
            }

            // data stream requested alone: the direct grant does not match the data stream, so nothing is relaxed
            iac = permission.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet(dataStreamName), metadata, fieldPermissionsCache);
            assertThat(iac.isGranted(), is(true));
            final IndicesAccessControl.IndexAccessControl shared = iac.getIndexPermissions(dataStreamName);
            assertThat(shared.getDocumentPermissions().getSingleSetOfQueries(), equalTo(query));
            for (String backingIndex : backingIndices) {
                assertSame("[" + backingIndex + "] for [" + dataStreamName + "]", shared, iac.getIndexPermissions(backingIndex));
            }

            // write index requested alone: both grants match it, allow-all wins
            iac = permission.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet(writeIndex), metadata, fieldPermissionsCache);
            assertThat(iac.isGranted(), is(true));
            assertThat(iac.getIndexPermissions(writeIndex).getDocumentPermissions().hasDocumentLevelPermissions(), is(false));
        }
    }

    /**
     * When the direct grant on a requested backing index adds nothing to what the data stream grant already gives
     * (same DLS query, no FLS), the merge keeps one of the two equal inputs rather than building a third. Which one
     * survives depends on the iteration order of the requested names, so the requested backing index is only
     * guaranteed an {@code IndexAccessControl} equal to the data stream's, while its siblings keep sharing the data
     * stream's instance.
     */
    public void testDirectGrantCoveredByDataStreamGrantKeepsSharedInstance() {
        final String dataStreamName = randomAlphaOfLength(6);
        final ProjectMetadata metadata = dataStreamProjectMetadata(dataStreamName, randomIntBetween(2, 5), false);
        final List<String> backingIndices = backingIndexNames(metadata, dataStreamName);
        final String requestedBackingIndex = randomFrom(backingIndices);
        final BytesReference query = new BytesArray("{\"term\":{\"tenant\":\"a\"}}");
        // two distinct but equal query sets, as two role groups would carry
        final IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.READ,
            FieldPermissions.DEFAULT,
            Collections.singleton(query),
            false,
            dataStreamName
        )
            .addGroup(
                IndexPrivilege.READ,
                FieldPermissions.DEFAULT,
                Set.of(new BytesArray(query.utf8ToString())),
                false,
                requestedBackingIndex
            )
            .build();

        final IndicesAccessControl iac = permission.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet(dataStreamName, requestedBackingIndex),
            metadata,
            new FieldPermissionsCache(Settings.EMPTY)
        );
        assertThat(iac.isGranted(), is(true));
        final IndicesAccessControl.IndexAccessControl dataStreamAccess = iac.getIndexPermissions(dataStreamName);
        assertThat(dataStreamAccess.getDocumentPermissions().getSingleSetOfQueries(), equalTo(Set.of(query)));
        assertThat(iac.getIndexPermissions(requestedBackingIndex), equalTo(dataStreamAccess));
        for (String backingIndex : backingIndices) {
            if (backingIndex.equals(requestedBackingIndex) == false) {
                assertSame("[" + backingIndex + "]", dataStreamAccess, iac.getIndexPermissions(backingIndex));
            }
        }
    }

    /**
     * Two aliases over one index, each with its own DLS and FLS, both requested: the index gets the union of both
     * grants, while each alias entry carries only the grants of the groups that matched that alias. Before the
     * refactoring the alias entries were written by reference from the index and could pick up each other's grants.
     */
    public void testAliasesOverSameIndexGetOwnEntriesWhileIndexGetsUnion() {
        final IndexMetadata.Builder indexBuilder = IndexMetadata.builder("_index")
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .putAlias(AliasMetadata.builder("_alias1"))
            .putAlias(AliasMetadata.builder("_alias2"));
        final ProjectMetadata metadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexBuilder.build(), true).build();
        final Set<BytesReference> query1 = Collections.singleton(new BytesArray("{\"term\":{\"tenant\":\"1\"}}"));
        final Set<BytesReference> query2 = Collections.singleton(new BytesArray("{\"term\":{\"tenant\":\"2\"}}"));
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        final IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.READ,
            new FieldPermissions(fieldPermissionDef(new String[] { "field1" }, null)),
            query1,
            false,
            "_alias1"
        )
            .addGroup(
                IndexPrivilege.READ,
                new FieldPermissions(fieldPermissionDef(new String[] { "field2" }, null)),
                query2,
                false,
                "_alias2"
            )
            .build();

        IndicesAccessControl iac = permission.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet("_alias1", "_alias2"),
            metadata,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(true));
        final IndicesAccessControl.IndexAccessControl indexAccess = iac.getIndexPermissions("_index");
        assertThat(indexAccess.getDocumentPermissions().getSingleSetOfQueries(), equalTo(Sets.union(query1, query2)));
        assertThat(indexAccess.getFieldPermissions().grantsAccessTo("field1"), is(true));
        assertThat(indexAccess.getFieldPermissions().grantsAccessTo("field2"), is(true));
        final IndicesAccessControl.IndexAccessControl alias1Access = iac.getIndexPermissions("_alias1");
        assertThat(alias1Access.getDocumentPermissions().getSingleSetOfQueries(), equalTo(query1));
        assertThat(alias1Access.getFieldPermissions().grantsAccessTo("field1"), is(true));
        assertThat(alias1Access.getFieldPermissions().grantsAccessTo("field2"), is(false));
        final IndicesAccessControl.IndexAccessControl alias2Access = iac.getIndexPermissions("_alias2");
        assertThat(alias2Access.getDocumentPermissions().getSingleSetOfQueries(), equalTo(query2));
        assertThat(alias2Access.getFieldPermissions().grantsAccessTo("field1"), is(false));
        assertThat(alias2Access.getFieldPermissions().grantsAccessTo("field2"), is(true));

        // one alias requested alone: the index and the alias share the alias' grants
        iac = permission.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("_alias1"), metadata, fieldPermissionsCache);
        assertThat(iac.isGranted(), is(true));
        assertSame(iac.getIndexPermissions("_alias1"), iac.getIndexPermissions("_index"));
        assertThat(iac.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), equalTo(query1));
        assertThat(iac.getIndexPermissions("_alias2"), is(nullValue()));
    }

    /**
     * An implicit DLS grant on the data stream and an explicit DLS grant on one requested backing index: only the
     * entry where both contribute is explicit (and carries both queries); the data stream entry and the other backing
     * indices stay implicit and keep sharing one instance.
     */
    public void testImplicitDataStreamGrantMergedWithExplicitBackingIndexGrantIsExplicitOnlyForThatIndex() {
        final String dataStreamName = randomAlphaOfLength(6);
        final ProjectMetadata metadata = dataStreamProjectMetadata(dataStreamName, randomIntBetween(3, 6), false);
        final List<String> backingIndices = backingIndexNames(metadata, dataStreamName);
        final String requestedBackingIndex = randomFrom(backingIndices);
        final Set<BytesReference> implicitQuery = Collections.singleton(new BytesArray("{\"term\":{\"clearance\":\"public\"}}"));
        final Set<BytesReference> explicitQuery = Collections.singleton(new BytesArray("{\"term\":{\"tenant\":\"a\"}}"));
        final IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.READ,
            FieldPermissions.DEFAULT,
            implicitQuery,
            false,
            true,
            dataStreamName
        ).addGroup(IndexPrivilege.READ, FieldPermissions.DEFAULT, explicitQuery, false, false, requestedBackingIndex).build();

        final IndicesAccessControl iac = permission.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet(dataStreamName, requestedBackingIndex),
            metadata,
            new FieldPermissionsCache(Settings.EMPTY)
        );
        assertThat(iac.isGranted(), is(true));
        final IndicesAccessControl.IndexAccessControl dataStreamAccess = iac.getIndexPermissions(dataStreamName);
        assertThat(dataStreamAccess.getDocumentPermissions().getSingleSetOfQueries(), equalTo(implicitQuery));
        assertThat(dataStreamAccess.isDlsFlsImplicit(), is(true));
        final IndicesAccessControl.IndexAccessControl mergedAccess = iac.getIndexPermissions(requestedBackingIndex);
        assertThat(mergedAccess.getDocumentPermissions().getSingleSetOfQueries(), equalTo(Sets.union(implicitQuery, explicitQuery)));
        assertThat(mergedAccess.isDlsFlsImplicit(), is(false));
        for (String backingIndex : backingIndices) {
            if (backingIndex.equals(requestedBackingIndex) == false) {
                assertSame("[" + backingIndex + "]", dataStreamAccess, iac.getIndexPermissions(backingIndex));
            }
        }
    }

    /**
     * The {@code ::failures} counterpart of the backing index scenario: a failure store grant with DLS on the data
     * stream and an unrestricted direct grant on one failure index. The {@code <data stream>::failures} entry and the
     * other failure indices keep the DLS and share one instance. The requested failure index is relaxed only when it
     * is itself requested.
     */
    public void testDirectlyRequestedFailureIndexWithUnrestrictedGrantIsRelaxedWithoutAffectingFailuresEntry() {
        final String dataStreamName = randomAlphaOfLength(6);
        final ProjectMetadata metadata = dataStreamProjectMetadata(dataStreamName, randomIntBetween(2, 4), true);
        final List<String> failureIndices = metadata.dataStreams()
            .get(dataStreamName)
            .getFailureIndices()
            .stream()
            .map(Index::getName)
            .toList();
        final String requestedFailureIndex = randomFrom(failureIndices);
        final Set<BytesReference> query = Collections.singleton(new BytesArray("{\"term\":{\"document.id\":\"1\"}}"));
        final IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.READ_FAILURE_STORE,
            FieldPermissions.DEFAULT,
            query,
            false,
            dataStreamName
        ).addGroup(IndexPrivilege.READ, FieldPermissions.DEFAULT, null, false, requestedFailureIndex).build();
        final FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        final String failuresName = dataStreamName + "::failures";

        IndicesAccessControl iac = permission.authorize(
            TransportSearchAction.TYPE.name(),
            Sets.newHashSet(failuresName, requestedFailureIndex),
            metadata,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(true));
        final IndicesAccessControl.IndexAccessControl failuresAccess = iac.getIndexPermissions(failuresName);
        assertThat(failuresAccess, is(notNullValue()));
        assertThat(failuresAccess.getDocumentPermissions().getSingleSetOfQueries(), equalTo(query));
        assertThat(iac.getIndexPermissions(requestedFailureIndex).getDocumentPermissions().hasDocumentLevelPermissions(), is(false));
        for (String failureIndex : failureIndices) {
            if (failureIndex.equals(requestedFailureIndex) == false) {
                assertSame("[" + failureIndex + "]", failuresAccess, iac.getIndexPermissions(failureIndex));
            }
        }

        // ::failures requested alone: every failure index keeps the DLS
        iac = permission.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet(failuresName), metadata, fieldPermissionsCache);
        assertThat(iac.isGranted(), is(true));
        for (String failureIndex : failureIndices) {
            assertSame("[" + failureIndex + "]", iac.getIndexPermissions(failuresName), iac.getIndexPermissions(failureIndex));
        }
    }

    /** A project with one data stream of {@code numBackingIndices} backing indices and, if requested, as many failure indices. */
    private static ProjectMetadata dataStreamProjectMetadata(String dataStreamName, int numBackingIndices, boolean withFailureStore) {
        return DataStreamTestHelper.getProjectWithDataStreams(
            List.of(new Tuple<>(dataStreamName, numBackingIndices)),
            List.of(),
            System.currentTimeMillis(),
            Settings.EMPTY,
            1,
            false,
            withFailureStore
        );
    }

    private static List<String> backingIndexNames(ProjectMetadata metadata, String dataStreamName) {
        return metadata.dataStreams().get(dataStreamName).getIndices().stream().map(Index::getName).toList();
    }

    /**
     * Data stream names for which a {@code HashMap} of the requested names (the data stream and its write index, i.e.
     * backing index number {@code writeIndexNumber}) iterates the backing index first, and names for which it
     * iterates the data stream first. Mirrors the map construction in {@code IndicesPermission#authorize} so the
     * caller exercises both orders. The authorization result must not depend on either.
     */
    private static List<String> dataStreamNamesCoveringBothIterationOrders(int writeIndexNumber) {
        String dataStreamFirst = null;
        String backingIndexFirst = null;
        for (int attempt = 0; attempt < 1000 && (dataStreamFirst == null || backingIndexFirst == null); attempt++) {
            final String candidate = randomAlphaOfLengthBetween(3, 12);
            final Map<String, Boolean> probe = Maps.newMapWithExpectedSize(2);
            probe.put(candidate, true);
            probe.put(DataStream.getDefaultBackingIndexName(candidate, writeIndexNumber), true);
            if (probe.keySet().iterator().next().equals(candidate)) {
                dataStreamFirst = candidate;
            } else {
                backingIndexFirst = candidate;
            }
        }
        assertThat("could not find names covering both iteration orders", dataStreamFirst, is(notNullValue()));
        assertThat("could not find names covering both iteration orders", backingIndexFirst, is(notNullValue()));
        return List.of(backingIndexFirst, dataStreamFirst);
    }

    public void testAuthorizationForMappingUpdates() {
        final Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        final ProjectMetadata.Builder projBuilder = ProjectMetadata.builder(randomProjectIdOrDefault())
            .put(new IndexMetadata.Builder("test1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
            .put(new IndexMetadata.Builder("test_write1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true);

        int numBackingIndices = randomIntBetween(1, 3);
        List<IndexMetadata> backingIndices = new ArrayList<>();
        for (int backingIndexNumber = 1; backingIndexNumber <= numBackingIndices; backingIndexNumber++) {
            backingIndices.add(createBackingIndexMetadata(DataStream.getDefaultBackingIndexName("test_write2", backingIndexNumber)));
        }
        DataStream ds = DataStreamTestHelper.newInstance(
            "test_write2",
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList())
        );
        projBuilder.put(ds);
        for (IndexMetadata index : backingIndices) {
            projBuilder.put(index, false);
        }

        ProjectMetadata metadata = projBuilder.build();

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        IndicesPermission core = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.INDEX,
            FieldPermissions.DEFAULT,
            null,
            randomBoolean(),
            "test*"
        )
            .addGroup(
                IndexPrivilege.WRITE,
                new FieldPermissions(fieldPermissionDef(null, new String[] { "denied_field" })),
                null,
                randomBoolean(),
                "test_write*"
            )
            .build();
        IndicesAccessControl iac = core.authorize(
            TransportPutMappingAction.TYPE.name(),
            Sets.newHashSet("test1", "test_write1"),
            metadata,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(true));
        assertThat(iac.getIndexPermissions("test1"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("test1"), is(true));
        assertThat(iac.getIndexPermissions("test_write1"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("test_write1"), is(true));
        assertWarnings(
            "the index privilege [index] allowed the update mapping action ["
                + TransportPutMappingAction.TYPE.name()
                + "] on "
                + "index [test1], this privilege will not permit mapping updates in the next major release - "
                + "users who require access to update mappings must be granted explicit privileges",
            "the index privilege [index] allowed the update mapping action ["
                + TransportPutMappingAction.TYPE.name()
                + "] on "
                + "index [test_write1], this privilege will not permit mapping updates in the next major release - "
                + "users who require access to update mappings must be granted explicit privileges",
            "the index privilege [write] allowed the update mapping action ["
                + TransportPutMappingAction.TYPE.name()
                + "] on "
                + "index [test_write1], this privilege will not permit mapping updates in the next major release - "
                + "users who require access to update mappings must be granted explicit privileges"
        );
        iac = core.authorize(
            TransportAutoPutMappingAction.TYPE.name(),
            Sets.newHashSet("test1", "test_write1"),
            metadata,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(true));
        assertThat(iac.getIndexPermissions("test1"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("test1"), is(true));
        assertThat(iac.getIndexPermissions("test_write1"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("test_write1"), is(true));
        assertWarnings(
            "the index privilege [index] allowed the update mapping action ["
                + TransportAutoPutMappingAction.TYPE.name()
                + "] on "
                + "index [test1], this privilege will not permit mapping updates in the next major release - "
                + "users who require access to update mappings must be granted explicit privileges"
        );

        iac = core.authorize(TransportAutoPutMappingAction.TYPE.name(), Sets.newHashSet("test_write2"), metadata, fieldPermissionsCache);
        assertThat(iac.isGranted(), is(true));
        assertThat(iac.getIndexPermissions("test_write2"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("test_write2"), is(true));
        iac = core.authorize(TransportPutMappingAction.TYPE.name(), Sets.newHashSet("test_write2"), metadata, fieldPermissionsCache);
        assertThat(iac.getIndexPermissions("test_write2"), is(nullValue()));
        assertThat(iac.hasIndexPermissions("test_write2"), is(false));
        iac = core.authorize(
            TransportAutoPutMappingAction.TYPE.name(),
            Sets.newHashSet(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList())),
            metadata,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(true));
        for (IndexMetadata im : backingIndices) {
            assertThat(iac.getIndexPermissions(im.getIndex().getName()), is(notNullValue()));
            assertThat(iac.hasIndexPermissions(im.getIndex().getName()), is(true));
        }
        iac = core.authorize(
            TransportPutMappingAction.TYPE.name(),
            Sets.newHashSet(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList())),
            metadata,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(false));
        for (IndexMetadata im : backingIndices) {
            assertThat(iac.getIndexPermissions(im.getIndex().getName()), is(nullValue()));
            assertThat(iac.hasIndexPermissions(im.getIndex().getName()), is(false));
        }
    }

    public void testIndicesPermissionHasFieldOrDocumentLevelSecurity() {
        // Make sure we have at least one of fieldPermissions and documentPermission
        final FieldPermissions fieldPermissions = randomBoolean()
            ? new FieldPermissions(new FieldPermissionsDefinition(Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY))
            : FieldPermissions.DEFAULT;
        final Set<BytesReference> queries;
        if (fieldPermissions == FieldPermissions.DEFAULT) {
            queries = Set.of(new BytesArray("a query"));
        } else {
            queries = randomBoolean() ? Set.of(new BytesArray("a query")) : null;
        }

        final IndicesPermission indicesPermission1 = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            fieldPermissions,
            queries,
            randomBoolean(),
            "*"
        ).build();
        assertThat(indicesPermission1.hasFieldOrDocumentLevelSecurity(), is(true));

        // IsTotal means no DLS/FLS
        final IndicesPermission indicesPermission2 = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            null,
            true,
            "*"
        ).build();
        assertThat(indicesPermission2.hasFieldOrDocumentLevelSecurity(), is(false));

        // IsTotal means NO DLS/FLS even when there is another group that has DLS/FLS
        final IndicesPermission indicesPermission3 = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            null,
            true,
            "*"
        ).addGroup(IndexPrivilege.NONE, fieldPermissions, queries, randomBoolean(), "*").build();
        assertThat(indicesPermission3.hasFieldOrDocumentLevelSecurity(), is(false));
    }

    public void testExplicitDlsIsNotMarkedDlsFlsImplicit() {
        final ProjectMetadata pmd = singleIndexProjectMetadata("_index");
        final FieldPermissionsCache fpc = new FieldPermissionsCache(Settings.EMPTY);
        final Set<BytesReference> query = Collections.singleton(new BytesArray("{}"));

        final IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            query,
            false,
            false, // explicit DLS
            "_index"
        ).build();

        final IndicesAccessControl iac = permission.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("_index"), pmd, fpc);
        final IndicesAccessControl.IndexAccessControl indexAccess = iac.getIndexPermissions("_index");
        assertThat(indexAccess, notNullValue());
        assertThat(indexAccess.getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat("explicit DLS must not be marked implicit", indexAccess.isDlsFlsImplicit(), is(false));
    }

    public void testImplicitDlsIsMarkedDlsFlsImplicit() {
        final ProjectMetadata pmd = singleIndexProjectMetadata("_index");
        final FieldPermissionsCache fpc = new FieldPermissionsCache(Settings.EMPTY);
        final Set<BytesReference> query = Collections.singleton(new BytesArray("{}"));

        final IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            query,
            false,
            true, // implicit DLS
            "_index"
        ).build();

        final IndicesAccessControl iac = permission.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("_index"), pmd, fpc);
        final IndicesAccessControl.IndexAccessControl indexAccess = iac.getIndexPermissions("_index");
        assertThat(indexAccess, notNullValue());
        assertThat(indexAccess.getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat("implicit DLS must be marked implicit", indexAccess.isDlsFlsImplicit(), is(true));
    }

    public void testImplicitFlsIsMarkedDlsFlsImplicit() {
        final ProjectMetadata pmd = singleIndexProjectMetadata("_index");
        final FieldPermissionsCache fpc = new FieldPermissionsCache(Settings.EMPTY);
        final FieldPermissions fls = new FieldPermissions(fieldPermissionDef(new String[] { "_field" }, null));

        final IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            fls,
            null,
            false,
            true, // implicit FLS
            "_index"
        ).build();

        final IndicesAccessControl iac = permission.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("_index"), pmd, fpc);
        final IndicesAccessControl.IndexAccessControl indexAccess = iac.getIndexPermissions("_index");
        assertThat(indexAccess, notNullValue());
        assertThat(indexAccess.getFieldPermissions().hasFieldLevelSecurity(), is(true));
        assertThat("implicit FLS must be marked implicit", indexAccess.isDlsFlsImplicit(), is(true));
    }

    public void testMixedExplicitAndImplicitDlsFlsIsNotMarkedDlsFlsImplicit() {
        final ProjectMetadata pmd = singleIndexProjectMetadata("_index");
        final FieldPermissionsCache fpc = new FieldPermissionsCache(Settings.EMPTY);
        final Set<BytesReference> query = Collections.singleton(new BytesArray("{}"));
        final FieldPermissions fls = new FieldPermissions(fieldPermissionDef(new String[] { "_field" }, null));

        // Same index covered by both an explicit DLS group and an implicit FLS group: explicit wins,
        // and the resulting IAC must report not-implicit so license enforcement still applies.
        final IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            query,
            false,
            false, // explicit DLS contributor
            "_index"
        ).addGroup(IndexPrivilege.READ, fls, null, false, true, "_index").build();

        final IndicesAccessControl iac = permission.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("_index"), pmd, fpc);
        final IndicesAccessControl.IndexAccessControl indexAccess = iac.getIndexPermissions("_index");
        assertThat(indexAccess, notNullValue());
        assertThat("any explicit DLS/FLS contributor must mark the IAC as not implicit", indexAccess.isDlsFlsImplicit(), is(false));
    }

    public void testIacWithoutDlsFlsIsNotMarkedDlsFlsImplicit() {
        final ProjectMetadata pmd = singleIndexProjectMetadata("_index");
        final FieldPermissionsCache fpc = new FieldPermissionsCache(Settings.EMPTY);

        // No DLS, no FLS: the flag should be false regardless of how the group was contributed.
        final IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            null,
            false,
            randomBoolean(),
            "_index"
        ).build();

        final IndicesAccessControl iac = permission.authorize(TransportSearchAction.TYPE.name(), Sets.newHashSet("_index"), pmd, fpc);
        final IndicesAccessControl.IndexAccessControl indexAccess = iac.getIndexPermissions("_index");
        assertThat(indexAccess, notNullValue());
        assertThat(indexAccess.getDocumentPermissions().hasDocumentLevelPermissions(), is(false));
        assertThat(indexAccess.getFieldPermissions().hasFieldLevelSecurity(), is(false));
        assertThat(indexAccess.isDlsFlsImplicit(), is(false));
    }

    private static ProjectMetadata singleIndexProjectMetadata(String indexName) {
        IndexMetadata.Builder imb = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 1, 1));
        return ProjectMetadata.builder(randomProjectIdOrDefault()).put(imb.build(), true).build();
    }

    public void testResourceAuthorizedPredicateForDatastreams() {
        String dataStreamName = "logs-datastream";
        final var project = DataStreamTestHelper.getProjectWithDataStreams(
            List.of(Tuple.tuple(dataStreamName, 1)),
            List.of(),
            Instant.now().toEpochMilli(),
            builder().build(),
            1
        );
        DataStream dataStream = project.dataStreams().get(dataStreamName);
        IndexAbstraction backingIndex = new IndexAbstraction.ConcreteIndex(
            DataStreamTestHelper.createBackingIndex(dataStreamName, 1).build(),
            dataStream
        );
        IndexAbstraction concreteIndex = new IndexAbstraction.ConcreteIndex(
            IndexMetadata.builder("logs-index").settings(indexSettings(IndexVersion.current(), 1, 0)).build()
        );
        AliasMetadata aliasMetadata = new AliasMetadata.Builder("logs-alias").build();
        IndexAbstraction alias = new IndexAbstraction.Alias(
            aliasMetadata,
            List.of(
                IndexMetadata.builder("logs-index").settings(indexSettings(IndexVersion.current(), 1, 0)).putAlias(aliasMetadata).build()
            )
        );
        IndicesPermission.IsResourceAuthorizedPredicate predicate = new IndicesPermission.IsResourceAuthorizedPredicate(
            StringMatcher.of("other"),
            StringMatcher.of(),
            StringMatcher.of(dataStreamName, backingIndex.getName(), concreteIndex.getName(), alias.getName())
        );
        assertThat(predicate.test(dataStream), is(false));
        // test authorization for a missing resource with the datastream's name
        assertThat(predicate.test(dataStream.getName(), null, IndexComponentSelector.DATA), is(true));
        assertThat(predicate.test(backingIndex), is(false));
        // test authorization for a missing resource with the backing index's name
        assertThat(predicate.test(backingIndex.getName(), null, IndexComponentSelector.DATA), is(true));
        assertThat(predicate.test(concreteIndex), is(true));
        assertThat(predicate.test(alias), is(true));
    }

    public void testResourceAuthorizedPredicateAnd() {
        IndicesPermission.IsResourceAuthorizedPredicate predicate1 = new IndicesPermission.IsResourceAuthorizedPredicate(
            StringMatcher.of("c", "a"),
            StringMatcher.of(),
            StringMatcher.of("b", "d")
        );
        IndicesPermission.IsResourceAuthorizedPredicate predicate2 = new IndicesPermission.IsResourceAuthorizedPredicate(
            StringMatcher.of("c", "b"),
            StringMatcher.of(),
            StringMatcher.of("a", "d")
        );
        final var project = DataStreamTestHelper.getProjectWithDataStreams(
            List.of(Tuple.tuple("a", 1), Tuple.tuple("b", 1), Tuple.tuple("c", 1), Tuple.tuple("d", 1)),
            List.of(),
            Instant.now().toEpochMilli(),
            builder().build(),
            1
        );
        DataStream dataStreamA = project.dataStreams().get("a");
        DataStream dataStreamB = project.dataStreams().get("b");
        DataStream dataStreamC = project.dataStreams().get("c");
        DataStream dataStreamD = project.dataStreams().get("d");
        IndexAbstraction concreteIndexA = concreteIndexAbstraction("a");
        IndexAbstraction concreteIndexB = concreteIndexAbstraction("b");
        IndexAbstraction concreteIndexC = concreteIndexAbstraction("c");
        IndexAbstraction concreteIndexD = concreteIndexAbstraction("d");
        IndicesPermission.IsResourceAuthorizedPredicate predicate = predicate1.and(predicate2);
        assertThat(predicate.test(dataStreamA), is(false));
        assertThat(predicate.test(dataStreamB), is(false));
        assertThat(predicate.test(dataStreamC), is(true));
        assertThat(predicate.test(dataStreamD), is(false));
        assertThat(predicate.test(concreteIndexA), is(true));
        assertThat(predicate.test(concreteIndexB), is(true));
        assertThat(predicate.test(concreteIndexC), is(true));
        assertThat(predicate.test(concreteIndexD), is(true));
    }

    public void testResourceAuthorizedPredicateAndWithFailures() {
        IndicesPermission.IsResourceAuthorizedPredicate predicate1 = new IndicesPermission.IsResourceAuthorizedPredicate(
            StringMatcher.of("c", "a"),
            StringMatcher.of("e", "f"),
            StringMatcher.of("b", "d")
        );
        IndicesPermission.IsResourceAuthorizedPredicate predicate2 = new IndicesPermission.IsResourceAuthorizedPredicate(
            StringMatcher.of("c", "b"),
            StringMatcher.of("a", "f", "g"),
            StringMatcher.of("a", "d")
        );
        final var project = DataStreamTestHelper.getProjectWithDataStreams(
            List.of(
                Tuple.tuple("a", 1),
                Tuple.tuple("b", 1),
                Tuple.tuple("c", 1),
                Tuple.tuple("d", 1),
                Tuple.tuple("e", 1),
                Tuple.tuple("f", 1)
            ),
            List.of(),
            Instant.now().toEpochMilli(),
            builder().build(),
            1
        );
        DataStream dataStreamA = project.dataStreams().get("a");
        DataStream dataStreamB = project.dataStreams().get("b");
        DataStream dataStreamC = project.dataStreams().get("c");
        DataStream dataStreamD = project.dataStreams().get("d");
        DataStream dataStreamE = project.dataStreams().get("e");
        DataStream dataStreamF = project.dataStreams().get("f");
        IndexAbstraction concreteIndexA = concreteIndexAbstraction("a");
        IndexAbstraction concreteIndexB = concreteIndexAbstraction("b");
        IndexAbstraction concreteIndexC = concreteIndexAbstraction("c");
        IndexAbstraction concreteIndexD = concreteIndexAbstraction("d");
        IndexAbstraction concreteIndexE = concreteIndexAbstraction("e");
        IndexAbstraction concreteIndexF = concreteIndexAbstraction("f");
        IndicesPermission.IsResourceAuthorizedPredicate predicate = predicate1.and(predicate2);
        assertThat(predicate.test(dataStreamA), is(false));
        assertThat(predicate.test(dataStreamB), is(false));
        assertThat(predicate.test(dataStreamC), is(true));
        assertThat(predicate.test(dataStreamD), is(false));
        assertThat(predicate.test(dataStreamE), is(false));
        assertThat(predicate.test(dataStreamF), is(false));

        assertThat(predicate.test(dataStreamA, IndexComponentSelector.FAILURES), is(false));
        assertThat(predicate.test(dataStreamB, IndexComponentSelector.FAILURES), is(false));
        assertThat(predicate.test(dataStreamC, IndexComponentSelector.FAILURES), is(false));
        assertThat(predicate.test(dataStreamD, IndexComponentSelector.FAILURES), is(false));
        assertThat(predicate.test(dataStreamE, IndexComponentSelector.FAILURES), is(false));
        assertThat(predicate.test(dataStreamF, IndexComponentSelector.FAILURES), is(true));

        assertThat(predicate.test(concreteIndexA), is(true));
        assertThat(predicate.test(concreteIndexB), is(true));
        assertThat(predicate.test(concreteIndexC), is(true));
        assertThat(predicate.test(concreteIndexD), is(true));
        assertThat(predicate.test(concreteIndexE), is(false));
        assertThat(predicate.test(concreteIndexF), is(false));

        assertThat(predicate.test(concreteIndexA, IndexComponentSelector.FAILURES), is(false));
        assertThat(predicate.test(concreteIndexB, IndexComponentSelector.FAILURES), is(false));
        assertThat(predicate.test(concreteIndexC, IndexComponentSelector.FAILURES), is(false));
        assertThat(predicate.test(concreteIndexD, IndexComponentSelector.FAILURES), is(false));
        assertThat(predicate.test(concreteIndexE, IndexComponentSelector.FAILURES), is(false));
        assertThat(predicate.test(concreteIndexF, IndexComponentSelector.FAILURES), is(true));
    }

    public void testCheckResourcePrivilegesWithTooComplexAutomaton() {
        IndicesPermission permission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            null,
            false,
            "my-index"
        ).build();

        var ex = expectThrows(
            IllegalArgumentException.class,
            () -> permission.checkResourcePrivileges(Set.of("****a*b?c**d**e*f??*g**h???i??*j*k*l*m*n???o*"), false, Set.of("read"), null)
        );
        assertThat(ex.getMessage(), containsString("index pattern [****a*b?c**d**e*f??*g**h???i??*j*k*l*m*n???o*]"));
        assertThat(ex.getCause(), instanceOf(TooComplexToDeterminizeException.class));
    }

    private static IndexAbstraction concreteIndexAbstraction(String name) {
        return new IndexAbstraction.ConcreteIndex(
            IndexMetadata.builder(name).settings(indexSettings(IndexVersion.current(), 1, 0)).build()
        );
    }

    private static IndexMetadata createBackingIndexMetadata(String name) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.hidden", true);

        IndexMetadata.Builder indexBuilder = IndexMetadata.builder(name)
            .settings(settingsBuilder)
            .state(IndexMetadata.State.OPEN)
            .numberOfShards(1)
            .numberOfReplicas(1);

        return indexBuilder.build();
    }

    private static FieldPermissionsDefinition fieldPermissionDef(String[] granted, String[] denied) {
        return new FieldPermissionsDefinition(granted, denied);
    }
}
