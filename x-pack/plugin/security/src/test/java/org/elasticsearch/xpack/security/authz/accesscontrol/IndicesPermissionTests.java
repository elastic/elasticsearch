/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
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
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.RESTRICTED_INDICES;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IndicesPermissionTests extends ESTestCase {

    public void testAuthorize() {
        IndexMetadata.Builder imbBuilder = IndexMetadata.builder("_index")
            .settings(indexSettings(Version.CURRENT, 1, 1))
            .putAlias(AliasMetadata.builder("_alias"));
        Metadata md = Metadata.builder().put(imbBuilder).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        SortedMap<String, IndexAbstraction> lookup = md.getIndicesLookup();

        // basics:
        Set<BytesReference> query = Collections.singleton(new BytesArray("{}"));
        String[] fields = new String[] { "_field" };
        Role role = Role.builder(RESTRICTED_INDICES, "_role")
            .add(new FieldPermissions(fieldPermissionDef(fields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_index")
            .build();
        IndicesAccessControl permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_index"), lookup, fieldPermissionsCache);
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
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_index"), lookup, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(false));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getListOfQueries(), nullValue());

        // no field level security:
        role = Role.builder(RESTRICTED_INDICES, "_role")
            .add(FieldPermissions.DEFAULT, query, IndexPrivilege.ALL, randomBoolean(), "_index")
            .build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_index"), lookup, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), hasSize(1));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), equalTo(query));

        // index group associated with an alias:
        role = Role.builder(RESTRICTED_INDICES, "_role")
            .add(new FieldPermissions(fieldPermissionDef(fields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_alias")
            .build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_alias"), lookup, fieldPermissionsCache);
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
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_alias"), lookup, fieldPermissionsCache);
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
            .settings(indexSettings(Version.CURRENT, 1, 1))
            .putAlias(AliasMetadata.builder("_alias"));
        md = Metadata.builder(md).put(imbBuilder1).build();
        lookup = md.getIndicesLookup();

        // match all fields with more than one permission
        Set<BytesReference> fooQuery = Collections.singleton(new BytesArray("{foo}"));
        allFields = randomFrom(new String[] { "*" }, new String[] { "foo", "*" }, new String[] { randomAlphaOfLengthBetween(1, 10), "*" });
        role = Role.builder(RESTRICTED_INDICES, "_role")
            .add(new FieldPermissions(fieldPermissionDef(allFields, null)), fooQuery, IndexPrivilege.ALL, randomBoolean(), "_alias")
            .add(new FieldPermissions(fieldPermissionDef(allFields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_alias")
            .build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_alias"), lookup, fieldPermissionsCache);
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

    public void testAuthorizeMultipleGroupsMixedDls() {
        IndexMetadata.Builder imbBuilder = IndexMetadata.builder("_index")
            .settings(indexSettings(Version.CURRENT, 1, 1))
            .putAlias(AliasMetadata.builder("_alias"));
        Metadata md = Metadata.builder().put(imbBuilder).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        SortedMap<String, IndexAbstraction> lookup = md.getIndicesLookup();

        Set<BytesReference> query = Collections.singleton(new BytesArray("{}"));
        String[] fields = new String[] { "_field" };
        Role role = Role.builder(RESTRICTED_INDICES, "_role")
            .add(new FieldPermissions(fieldPermissionDef(fields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_index")
            .add(new FieldPermissions(fieldPermissionDef(null, null)), null, IndexPrivilege.ALL, randomBoolean(), "*")
            .build();
        IndicesAccessControl permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_index"), lookup, fieldPermissionsCache);
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
        out.setTransportVersion(TransportVersion.CURRENT);
        indicesPrivileges = RoleDescriptor.IndicesPrivileges.builder();
        indicesPrivileges.grantedFields(allowed);
        indicesPrivileges.deniedFields(denied);
        indicesPrivileges.query("{match_all:{}}");
        indicesPrivileges.indices(readIndicesPrivileges.getIndices());
        indicesPrivileges.privileges("all", "read", "priv");
        indicesPrivileges.build().writeTo(out);
        out.close();
        in = out.bytes().streamInput();
        in.setTransportVersion(TransportVersion.CURRENT);
        RoleDescriptor.IndicesPrivileges readIndicesPrivileges2 = new RoleDescriptor.IndicesPrivileges(in);
        assertEquals(readIndicesPrivileges, readIndicesPrivileges2);
    }

    // tests that field permissions are merged correctly when we authorize with several groups and don't crash when an index has no group
    public void testCorePermissionAuthorize() {
        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final Metadata metadata = new Metadata.Builder().put(
            new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(),
            true
        ).put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true).build();
        SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();

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
        IndicesAccessControl iac = core.authorize(SearchAction.NAME, Sets.newHashSet("a1", "ba"), lookup, fieldPermissionsCache);
        assertTrue(iac.getIndexPermissions("a1").getFieldPermissions().grantsAccessTo("denied_field"));
        assertTrue(iac.getIndexPermissions("a1").getFieldPermissions().grantsAccessTo(randomAlphaOfLength(5)));
        // did not define anything for ba so we allow all
        assertFalse(iac.hasIndexPermissions("ba"));

        assertTrue(core.check(SearchAction.NAME));
        assertTrue(core.check(PutMappingAction.NAME));
        assertTrue(core.check(AutoPutMappingAction.NAME));
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
        iac = core.authorize(SearchAction.NAME, Sets.newHashSet("a1", "a2"), lookup, fieldPermissionsCache);
        assertFalse(iac.getIndexPermissions("a1").getFieldPermissions().hasFieldLevelSecurity());
        assertFalse(iac.getIndexPermissions("a2").getFieldPermissions().grantsAccessTo("denied_field2"));
        assertFalse(iac.getIndexPermissions("a2").getFieldPermissions().grantsAccessTo("denied_field"));
        assertTrue(iac.getIndexPermissions("a2").getFieldPermissions().grantsAccessTo(randomAlphaOfLength(5) + "_field"));
        assertTrue(iac.getIndexPermissions("a2").getFieldPermissions().grantsAccessTo(randomAlphaOfLength(5) + "_field2"));
        assertTrue(iac.getIndexPermissions("a2").getFieldPermissions().hasFieldLevelSecurity());

        assertTrue(core.check(SearchAction.NAME));
        assertTrue(core.check(PutMappingAction.NAME));
        assertTrue(core.check(AutoPutMappingAction.NAME));
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
                indices.toArray(Strings.EMPTY_ARRAY)
            )
        );
        assertThat(e.getMessage(), containsString(indices.get(0)));
        assertThat(e.getMessage(), containsString("too complex to evaluate"));
    }

    public void testSecurityIndicesPermissions() {
        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_6,
            TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7
        );
        final Metadata metadata = new Metadata.Builder().put(
            new IndexMetadata.Builder(internalSecurityIndex).settings(indexSettings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(new AliasMetadata.Builder(SecuritySystemIndices.SECURITY_MAIN_ALIAS).build())
                .build(),
            true
        ).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();

        // allow_restricted_indices: false
        IndicesPermission indicesPermission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            null,
            false,
            "*"
        ).build();
        IndicesAccessControl iac = indicesPermission.authorize(
            SearchAction.NAME,
            Sets.newHashSet(internalSecurityIndex, SecuritySystemIndices.SECURITY_MAIN_ALIAS),
            lookup,
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
            SearchAction.NAME,
            Sets.newHashSet(internalSecurityIndex, SecuritySystemIndices.SECURITY_MAIN_ALIAS),
            lookup,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(true));
        assertThat(iac.hasIndexPermissions(internalSecurityIndex), is(true));
        assertThat(iac.getIndexPermissions(internalSecurityIndex), is(notNullValue()));
        assertThat(iac.hasIndexPermissions(SecuritySystemIndices.SECURITY_MAIN_ALIAS), is(true));
        assertThat(iac.getIndexPermissions(SecuritySystemIndices.SECURITY_MAIN_ALIAS), is(notNullValue()));
    }

    public void testAsyncSearchIndicesPermissions() {
        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String asyncSearchIndex = XPackPlugin.ASYNC_RESULTS_INDEX + randomAlphaOfLengthBetween(0, 2);
        final Metadata metadata = new Metadata.Builder().put(
            new IndexMetadata.Builder(asyncSearchIndex).settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(),
            true
        ).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();

        // allow_restricted_indices: false
        IndicesPermission indicesPermission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.ALL,
            FieldPermissions.DEFAULT,
            null,
            false,
            "*"
        ).build();
        IndicesAccessControl iac = indicesPermission.authorize(
            SearchAction.NAME,
            Sets.newHashSet(asyncSearchIndex),
            lookup,
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
        iac = indicesPermission.authorize(SearchAction.NAME, Sets.newHashSet(asyncSearchIndex), lookup, fieldPermissionsCache);
        assertThat(iac.isGranted(), is(true));
        assertThat(iac.hasIndexPermissions(asyncSearchIndex), is(true));
        assertThat(iac.getIndexPermissions(asyncSearchIndex), is(notNullValue()));
    }

    public void testAuthorizationForBackingIndices() {
        Metadata.Builder builder = Metadata.builder();
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
        Metadata metadata = builder.build();

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();
        IndicesPermission indicesPermission = new IndicesPermission.Builder(RESTRICTED_INDICES).addGroup(
            IndexPrivilege.READ,
            FieldPermissions.DEFAULT,
            null,
            false,
            dataStreamName
        ).build();
        IndicesAccessControl iac = indicesPermission.authorize(
            SearchAction.NAME,
            Sets.newHashSet(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList())),
            lookup,
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
            randomFrom(PutMappingAction.NAME, AutoPutMappingAction.NAME),
            Sets.newHashSet(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList())),
            lookup,
            fieldPermissionsCache
        );

        assertThat(iac.isGranted(), is(false));
        for (IndexMetadata im : backingIndices) {
            assertThat(iac.getIndexPermissions(im.getIndex().getName()), is(nullValue()));
            assertThat(iac.hasIndexPermissions(im.getIndex().getName()), is(false));
        }
    }

    public void testAuthorizationForMappingUpdates() {
        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final Metadata.Builder metadata = new Metadata.Builder().put(
            new IndexMetadata.Builder("test1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(),
            true
        ).put(new IndexMetadata.Builder("test_write1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true);

        int numBackingIndices = randomIntBetween(1, 3);
        List<IndexMetadata> backingIndices = new ArrayList<>();
        for (int backingIndexNumber = 1; backingIndexNumber <= numBackingIndices; backingIndexNumber++) {
            backingIndices.add(createBackingIndexMetadata(DataStream.getDefaultBackingIndexName("test_write2", backingIndexNumber)));
        }
        DataStream ds = DataStreamTestHelper.newInstance(
            "test_write2",
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList())
        );
        metadata.put(ds);
        for (IndexMetadata index : backingIndices) {
            metadata.put(index, false);
        }

        SortedMap<String, IndexAbstraction> lookup = metadata.build().getIndicesLookup();

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
            PutMappingAction.NAME,
            Sets.newHashSet("test1", "test_write1"),
            lookup,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(true));
        assertThat(iac.getIndexPermissions("test1"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("test1"), is(true));
        assertThat(iac.getIndexPermissions("test_write1"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("test_write1"), is(true));
        assertWarnings(
            "the index privilege [index] allowed the update mapping action ["
                + PutMappingAction.NAME
                + "] on "
                + "index [test1], this privilege will not permit mapping updates in the next major release - "
                + "users who require access to update mappings must be granted explicit privileges",
            "the index privilege [index] allowed the update mapping action ["
                + PutMappingAction.NAME
                + "] on "
                + "index [test_write1], this privilege will not permit mapping updates in the next major release - "
                + "users who require access to update mappings must be granted explicit privileges",
            "the index privilege [write] allowed the update mapping action ["
                + PutMappingAction.NAME
                + "] on "
                + "index [test_write1], this privilege will not permit mapping updates in the next major release - "
                + "users who require access to update mappings must be granted explicit privileges"
        );
        iac = core.authorize(AutoPutMappingAction.NAME, Sets.newHashSet("test1", "test_write1"), lookup, fieldPermissionsCache);
        assertThat(iac.isGranted(), is(true));
        assertThat(iac.getIndexPermissions("test1"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("test1"), is(true));
        assertThat(iac.getIndexPermissions("test_write1"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("test_write1"), is(true));
        assertWarnings(
            "the index privilege [index] allowed the update mapping action ["
                + AutoPutMappingAction.NAME
                + "] on "
                + "index [test1], this privilege will not permit mapping updates in the next major release - "
                + "users who require access to update mappings must be granted explicit privileges"
        );

        iac = core.authorize(AutoPutMappingAction.NAME, Sets.newHashSet("test_write2"), lookup, fieldPermissionsCache);
        assertThat(iac.isGranted(), is(true));
        assertThat(iac.getIndexPermissions("test_write2"), is(notNullValue()));
        assertThat(iac.hasIndexPermissions("test_write2"), is(true));
        iac = core.authorize(PutMappingAction.NAME, Sets.newHashSet("test_write2"), lookup, fieldPermissionsCache);
        assertThat(iac.getIndexPermissions("test_write2"), is(nullValue()));
        assertThat(iac.hasIndexPermissions("test_write2"), is(false));
        iac = core.authorize(
            AutoPutMappingAction.NAME,
            Sets.newHashSet(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList())),
            lookup,
            fieldPermissionsCache
        );
        assertThat(iac.isGranted(), is(true));
        for (IndexMetadata im : backingIndices) {
            assertThat(iac.getIndexPermissions(im.getIndex().getName()), is(notNullValue()));
            assertThat(iac.hasIndexPermissions(im.getIndex().getName()), is(true));
        }
        iac = core.authorize(
            PutMappingAction.NAME,
            Sets.newHashSet(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList())),
            lookup,
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

    public void testResourceAuthorizedPredicateForDatastreams() {
        String dataStreamName = "logs-datastream";
        Metadata.Builder mb = Metadata.builder(
            DataStreamTestHelper.getClusterStateWithDataStreams(
                List.of(Tuple.tuple(dataStreamName, 1)),
                List.of(),
                Instant.now().toEpochMilli(),
                builder().build(),
                1
            ).getMetadata()
        );
        DataStream dataStream = mb.dataStream(dataStreamName);
        IndexAbstraction backingIndex = new IndexAbstraction.ConcreteIndex(
            DataStreamTestHelper.createBackingIndex(dataStreamName, 1).build(),
            dataStream
        );
        IndexAbstraction concreteIndex = new IndexAbstraction.ConcreteIndex(
            IndexMetadata.builder("logs-index").settings(indexSettings(Version.CURRENT, 1, 0)).build()
        );
        AliasMetadata aliasMetadata = new AliasMetadata.Builder("logs-alias").build();
        IndexAbstraction alias = new IndexAbstraction.Alias(
            aliasMetadata,
            List.of(IndexMetadata.builder("logs-index").settings(indexSettings(Version.CURRENT, 1, 0)).putAlias(aliasMetadata).build())
        );
        IndicesPermission.IsResourceAuthorizedPredicate predicate = new IndicesPermission.IsResourceAuthorizedPredicate(
            StringMatcher.of("other"),
            StringMatcher.of(dataStreamName, backingIndex.getName(), concreteIndex.getName(), alias.getName())
        );
        assertThat(predicate.test(dataStream), is(false));
        // test authorization for a missing resource with the datastream's name
        assertThat(predicate.test(dataStream.getName(), null), is(true));
        assertThat(predicate.test(backingIndex), is(false));
        // test authorization for a missing resource with the backing index's name
        assertThat(predicate.test(backingIndex.getName(), null), is(true));
        assertThat(predicate.test(concreteIndex), is(true));
        assertThat(predicate.test(alias), is(true));
    }

    public void testResourceAuthorizedPredicateAnd() {
        IndicesPermission.IsResourceAuthorizedPredicate predicate1 = new IndicesPermission.IsResourceAuthorizedPredicate(
            StringMatcher.of("c", "a"),
            StringMatcher.of("b", "d")
        );
        IndicesPermission.IsResourceAuthorizedPredicate predicate2 = new IndicesPermission.IsResourceAuthorizedPredicate(
            StringMatcher.of("c", "b"),
            StringMatcher.of("a", "d")
        );
        Metadata.Builder mb = Metadata.builder(
            DataStreamTestHelper.getClusterStateWithDataStreams(
                List.of(Tuple.tuple("a", 1), Tuple.tuple("b", 1), Tuple.tuple("c", 1), Tuple.tuple("d", 1)),
                List.of(),
                Instant.now().toEpochMilli(),
                builder().build(),
                1
            ).getMetadata()
        );
        DataStream dataStreamA = mb.dataStream("a");
        DataStream dataStreamB = mb.dataStream("b");
        DataStream dataStreamC = mb.dataStream("c");
        DataStream dataStreamD = mb.dataStream("d");
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

    private static IndexAbstraction concreteIndexAbstraction(String name) {
        return new IndexAbstraction.ConcreteIndex(IndexMetadata.builder(name).settings(indexSettings(Version.CURRENT, 1, 0)).build());
    }

    private static IndexMetadata createBackingIndexMetadata(String name) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
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
