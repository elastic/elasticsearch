/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IndicesPermissionTests extends ESTestCase {

    public void testAuthorize() {
        IndexMetadata.Builder imbBuilder = IndexMetadata.builder("_index")
                .settings(Settings.builder()
                                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                )
                .putAlias(AliasMetadata.builder("_alias"));
        Metadata md = Metadata.builder().put(imbBuilder).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        SortedMap<String, IndexAbstraction> lookup = md.getIndicesLookup();

        // basics:
        Set<BytesReference> query = Collections.singleton(new BytesArray("{}"));
        String[] fields = new String[]{"_field"};
        Role role = Role.builder("_role")
            .add(new FieldPermissions(fieldPermissionDef(fields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_index")
            .build();
        IndicesAccessControl permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_index"), lookup, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getQueries().size(), equalTo(1));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getQueries(), equalTo(query));

        // no document level security:
        role = Role.builder("_role")
            .add(new FieldPermissions(fieldPermissionDef(fields, null)), null, IndexPrivilege.ALL, randomBoolean(), "_index")
            .build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_index"), lookup, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(false));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getQueries(), nullValue());

        // no field level security:
        role = Role.builder("_role").add(new FieldPermissions(), query, IndexPrivilege.ALL, randomBoolean(), "_index").build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_index"), lookup, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getQueries().size(), equalTo(1));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getQueries(), equalTo(query));

        // index group associated with an alias:
        role = Role.builder("_role")
                .add(new FieldPermissions(fieldPermissionDef(fields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_alias")
                .build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_alias"), lookup, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getQueries().size(), equalTo(1));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getQueries(), equalTo(query));

        assertThat(permissions.getIndexPermissions("_alias"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_alias").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_alias").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().getQueries().size(), equalTo(1));
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().getQueries(), equalTo(query));

        // match all fields
        String[] allFields = randomFrom(new String[]{"*"}, new String[]{"foo", "*"},
        new String[]{randomAlphaOfLengthBetween(1, 10), "*"});
        role = Role.builder("_role")
            .add(new FieldPermissions(fieldPermissionDef(allFields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_alias")
            .build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_alias"), lookup, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getQueries().size(), equalTo(1));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getQueries(), equalTo(query));

        assertThat(permissions.getIndexPermissions("_alias"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_alias").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().getQueries().size(), equalTo(1));
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().getQueries(), equalTo(query));

        IndexMetadata.Builder imbBuilder1 = IndexMetadata.builder("_index_1")
                .settings(Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                )
                .putAlias(AliasMetadata.builder("_alias"));
        md = Metadata.builder(md).put(imbBuilder1).build();
        lookup = md.getIndicesLookup();

        // match all fields with more than one permission
        Set<BytesReference> fooQuery = Collections.singleton(new BytesArray("{foo}"));
        allFields = randomFrom(new String[]{"*"}, new String[]{"foo", "*"},
                new String[]{randomAlphaOfLengthBetween(1, 10), "*"});
        role = Role.builder("_role")
            .add(new FieldPermissions(fieldPermissionDef(allFields, null)), fooQuery, IndexPrivilege.ALL, randomBoolean(), "_alias")
            .add(new FieldPermissions(fieldPermissionDef(allFields, null)), query, IndexPrivilege.ALL, randomBoolean(), "_alias")
            .build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_alias"), lookup, fieldPermissionsCache);
        Set<BytesReference> bothQueries = Sets.union(fooQuery, query);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getQueries().size(), equalTo(2));
        assertThat(permissions.getIndexPermissions("_index").getDocumentPermissions().getQueries(), equalTo(bothQueries));

        assertThat(permissions.getIndexPermissions("_index_1"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index_1").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index_1").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_index_1").getDocumentPermissions().getQueries().size(), equalTo(2));
        assertThat(permissions.getIndexPermissions("_index_1").getDocumentPermissions().getQueries(), equalTo(bothQueries));

        assertThat(permissions.getIndexPermissions("_alias"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_alias").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().getQueries().size(), equalTo(2));
        assertThat(permissions.getIndexPermissions("_alias").getDocumentPermissions().getQueries(), equalTo(bothQueries));

    }

    public void testAuthorizeMultipleGroupsMixedDls() {
        IndexMetadata.Builder imbBuilder = IndexMetadata.builder("_index")
                .settings(Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                )
                .putAlias(AliasMetadata.builder("_alias"));
        Metadata md = Metadata.builder().put(imbBuilder).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        SortedMap<String, IndexAbstraction> lookup = md.getIndicesLookup();

        Set<BytesReference> query = Collections.singleton(new BytesArray("{}"));
        String[] fields = new String[]{"_field"};
        Role role = Role.builder("_role")
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
        String[] allowed = new String[]{randomAlphaOfLength(5) + "*", randomAlphaOfLength(5) + "*", randomAlphaOfLength(5) + "*"};
        String[] denied = new String[]{allowed[0] + randomAlphaOfLength(5), allowed[1] + randomAlphaOfLength(5),
                allowed[2] + randomAlphaOfLength(5)};
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
        out.setVersion(Version.CURRENT);
        indicesPrivileges = RoleDescriptor.IndicesPrivileges.builder();
        indicesPrivileges.grantedFields(allowed);
        indicesPrivileges.deniedFields(denied);
        indicesPrivileges.query("{match_all:{}}");
        indicesPrivileges.indices(readIndicesPrivileges.getIndices());
        indicesPrivileges.privileges("all", "read", "priv");
        indicesPrivileges.build().writeTo(out);
        out.close();
        in = out.bytes().streamInput();
        in.setVersion(Version.CURRENT);
        RoleDescriptor.IndicesPrivileges readIndicesPrivileges2 = new RoleDescriptor.IndicesPrivileges(in);
        assertEquals(readIndicesPrivileges, readIndicesPrivileges2);
    }

    // tests that field permissions are merged correctly when we authorize with several groups and don't crash when an index has no group
    public void testCorePermissionAuthorize() {
        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final Metadata metadata = new Metadata.Builder()
                .put(new IndexMetadata.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .build();
        SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        IndicesPermission.Group group1 = new IndicesPermission.Group(IndexPrivilege.ALL, new FieldPermissions(), null, randomBoolean(),
                "a1");
        IndicesPermission.Group group2 = new IndicesPermission.Group(IndexPrivilege.READ,
                new FieldPermissions(fieldPermissionDef(null, new String[]{"denied_field"})), null, randomBoolean(), "a1");
        IndicesPermission core = new IndicesPermission(group1, group2);
        Map<String, IndicesAccessControl.IndexAccessControl> authzMap =
                core.authorize(SearchAction.NAME, Sets.newHashSet("a1", "ba"), lookup, fieldPermissionsCache);
        assertTrue(authzMap.get("a1").getFieldPermissions().grantsAccessTo("denied_field"));
        assertTrue(authzMap.get("a1").getFieldPermissions().grantsAccessTo(randomAlphaOfLength(5)));
        // did not define anything for ba so we allow all
        assertFalse(authzMap.get("ba").getFieldPermissions().hasFieldLevelSecurity());

        assertTrue(core.check(SearchAction.NAME));
        assertTrue(core.check(PutMappingAction.NAME));
        assertTrue(core.check(AutoPutMappingAction.NAME));
        assertFalse(core.check("unknown"));

        // test with two indices
        group1 = new IndicesPermission.Group(IndexPrivilege.ALL, new FieldPermissions(), null, randomBoolean(), "a1");
        group2 = new IndicesPermission.Group(IndexPrivilege.ALL,
                new FieldPermissions(fieldPermissionDef(null, new String[]{"denied_field"})), null, randomBoolean(), "a1");
        IndicesPermission.Group group3 = new IndicesPermission.Group(IndexPrivilege.ALL,
                new FieldPermissions(fieldPermissionDef(new String[] { "*_field" }, new String[] { "denied_field" })), null,
                randomBoolean(), "a2");
        IndicesPermission.Group group4 = new IndicesPermission.Group(IndexPrivilege.ALL,
                new FieldPermissions(fieldPermissionDef(new String[] { "*_field2" }, new String[] { "denied_field2" })), null,
                randomBoolean(), "a2");
        core = new IndicesPermission(group1, group2, group3, group4);
        authzMap = core.authorize(SearchAction.NAME, Sets.newHashSet("a1", "a2"), lookup, fieldPermissionsCache);
        assertFalse(authzMap.get("a1").getFieldPermissions().hasFieldLevelSecurity());
        assertFalse(authzMap.get("a2").getFieldPermissions().grantsAccessTo("denied_field2"));
        assertFalse(authzMap.get("a2").getFieldPermissions().grantsAccessTo("denied_field"));
        assertTrue(authzMap.get("a2").getFieldPermissions().grantsAccessTo(randomAlphaOfLength(5) + "_field"));
        assertTrue(authzMap.get("a2").getFieldPermissions().grantsAccessTo(randomAlphaOfLength(5) + "_field2"));
        assertTrue(authzMap.get("a2").getFieldPermissions().hasFieldLevelSecurity());

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
        final ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class,
                () -> new IndicesPermission.Group(IndexPrivilege.ALL, new FieldPermissions(), null, randomBoolean(),
                        indices.toArray(Strings.EMPTY_ARRAY)));
        assertThat(e.getMessage(), containsString(indices.get(0)));
        assertThat(e.getMessage(), containsString("too complex to evaluate"));
    }

    public void testSecurityIndicesPermissions() {
        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String internalSecurityIndex = randomFrom(RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_6,
            RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7);
        final Metadata metadata = new Metadata.Builder()
                .put(new IndexMetadata.Builder(internalSecurityIndex)
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .putAlias(new AliasMetadata.Builder(RestrictedIndicesNames.SECURITY_MAIN_ALIAS).build())
                        .build(), true)
                .build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();

        // allow_restricted_indices: false
        IndicesPermission.Group group = new IndicesPermission.Group(IndexPrivilege.ALL, new FieldPermissions(), null, false, "*");
        Map<String, IndicesAccessControl.IndexAccessControl> authzMap = new IndicesPermission(group).authorize(SearchAction.NAME,
                Sets.newHashSet(internalSecurityIndex, RestrictedIndicesNames.SECURITY_MAIN_ALIAS), lookup,
                fieldPermissionsCache);
        assertThat(authzMap.get(internalSecurityIndex).isGranted(), is(false));
        assertThat(authzMap.get(RestrictedIndicesNames.SECURITY_MAIN_ALIAS).isGranted(), is(false));

        // allow_restricted_indices: true
        group = new IndicesPermission.Group(IndexPrivilege.ALL, new FieldPermissions(), null, true, "*");
        authzMap = new IndicesPermission(group).authorize(SearchAction.NAME,
                Sets.newHashSet(internalSecurityIndex, RestrictedIndicesNames.SECURITY_MAIN_ALIAS), lookup,
                fieldPermissionsCache);
        assertThat(authzMap.get(internalSecurityIndex).isGranted(), is(true));
        assertThat(authzMap.get(RestrictedIndicesNames.SECURITY_MAIN_ALIAS).isGranted(), is(true));
    }

    public void testAsyncSearchIndicesPermissions() {
        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final String asyncSearchIndex = RestrictedIndicesNames.ASYNC_SEARCH_PREFIX + randomAlphaOfLengthBetween(0, 2);
        final Metadata metadata = new Metadata.Builder()
                .put(new IndexMetadata.Builder(asyncSearchIndex)
                        .settings(indexSettings)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .build(), true)
                .build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();

        // allow_restricted_indices: false
        IndicesPermission.Group group = new IndicesPermission.Group(IndexPrivilege.ALL, new FieldPermissions(), null, false, "*");
        Map<String, IndicesAccessControl.IndexAccessControl> authzMap = new IndicesPermission(group).authorize(SearchAction.NAME,
                Sets.newHashSet(asyncSearchIndex), lookup, fieldPermissionsCache);
        assertThat(authzMap.get(asyncSearchIndex).isGranted(), is(false));

        // allow_restricted_indices: true
        group = new IndicesPermission.Group(IndexPrivilege.ALL, new FieldPermissions(), null, true, "*");
        authzMap = new IndicesPermission(group).authorize(SearchAction.NAME,
                Sets.newHashSet(asyncSearchIndex), lookup, fieldPermissionsCache);
        assertThat(authzMap.get(asyncSearchIndex).isGranted(), is(true));
    }

    public void testAuthorizationForBackingIndices() {
        Metadata.Builder builder = Metadata.builder();
        String dataStreamName = randomAlphaOfLength(6);
        int numBackingIndices = randomIntBetween(1, 3);
        List<IndexMetadata> backingIndices = new ArrayList<>();
        for (int backingIndexNumber = 1; backingIndexNumber <= numBackingIndices; backingIndexNumber++) {
            backingIndices.add(createIndexMetadata(DataStream.getDefaultBackingIndexName(dataStreamName, backingIndexNumber)));
        }
        DataStream ds = new DataStream(dataStreamName, createTimestampField("@timestamp"),
            backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()));
        builder.put(ds);
        for (IndexMetadata index : backingIndices) {
            builder.put(index, false);
        }
        Metadata metadata = builder.build();

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        SortedMap<String, IndexAbstraction> lookup = metadata.getIndicesLookup();
        IndicesPermission.Group group = new IndicesPermission.Group(IndexPrivilege.READ, new FieldPermissions(), null, false,
                dataStreamName);
        Map<String, IndicesAccessControl.IndexAccessControl> authzMap = new IndicesPermission(group).authorize(
                SearchAction.NAME,
                Sets.newHashSet(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList())),
                lookup,
                fieldPermissionsCache);

        for (IndexMetadata im : backingIndices) {
            assertThat(authzMap.get(im.getIndex().getName()).isGranted(), is(true));
        }

        group = new IndicesPermission.Group(IndexPrivilege.CREATE_DOC, new FieldPermissions(), null, false, dataStreamName);
        authzMap = new IndicesPermission(group).authorize(
                randomFrom(PutMappingAction.NAME, AutoPutMappingAction.NAME),
                Sets.newHashSet(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList())),
                lookup,
                fieldPermissionsCache);

        for (IndexMetadata im : backingIndices) {
            assertThat(authzMap.get(im.getIndex().getName()).isGranted(), is(false));
        }
    }

    public void testAuthorizationForMappingUpdates() {
        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final Metadata.Builder metadata = new Metadata.Builder()
                .put(new IndexMetadata.Builder("test1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetadata.Builder("test_write1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true);

        int numBackingIndices = randomIntBetween(1, 3);
        List<IndexMetadata> backingIndices = new ArrayList<>();
        for (int backingIndexNumber = 1; backingIndexNumber <= numBackingIndices; backingIndexNumber++) {
            backingIndices.add(createIndexMetadata(DataStream.getDefaultBackingIndexName("test_write2", backingIndexNumber)));
        }
        DataStream ds = new DataStream("test_write2", createTimestampField("@timestamp"),
                backingIndices.stream().map(IndexMetadata::getIndex).collect(Collectors.toList()));
        metadata.put(ds);
        for (IndexMetadata index : backingIndices) {
            metadata.put(index, false);
        }

        SortedMap<String, IndexAbstraction> lookup = metadata.build().getIndicesLookup();

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        IndicesPermission.Group group1 = new IndicesPermission.Group(IndexPrivilege.INDEX, new FieldPermissions(), null, randomBoolean(),
                "test*");
        IndicesPermission.Group group2 = new IndicesPermission.Group(IndexPrivilege.WRITE,
                new FieldPermissions(fieldPermissionDef(null, new String[]{"denied_field"})), null, randomBoolean(), "test_write*");
        IndicesPermission core = new IndicesPermission(group1, group2);
        Map<String, IndicesAccessControl.IndexAccessControl> authzMap =
                core.authorize(PutMappingAction.NAME, Sets.newHashSet("test1", "test_write1"), lookup, fieldPermissionsCache);
        assertThat(authzMap.get("test1").isGranted(), is(true));
        assertThat(authzMap.get("test_write1").isGranted(), is(true));
        assertWarnings("the index privilege [index] allowed the update mapping action [" + PutMappingAction.NAME + "] on " +
                        "index [test1], this privilege will not permit mapping updates in the next major release - " +
                        "users who require access to update mappings must be granted explicit privileges",
                "the index privilege [index] allowed the update mapping action [" + PutMappingAction.NAME + "] on " +
                        "index [test_write1], this privilege will not permit mapping updates in the next major release - " +
                        "users who require access to update mappings must be granted explicit privileges",
                "the index privilege [write] allowed the update mapping action [" + PutMappingAction.NAME + "] on " +
                        "index [test_write1], this privilege will not permit mapping updates in the next major release - " +
                        "users who require access to update mappings must be granted explicit privileges"
        );
        authzMap = core.authorize(AutoPutMappingAction.NAME, Sets.newHashSet("test1", "test_write1"), lookup, fieldPermissionsCache);
        assertThat(authzMap.get("test1").isGranted(), is(true));
        assertThat(authzMap.get("test_write1").isGranted(), is(true));
        assertWarnings("the index privilege [index] allowed the update mapping action [" + AutoPutMappingAction.NAME + "] on " +
                        "index [test1], this privilege will not permit mapping updates in the next major release - " +
                        "users who require access to update mappings must be granted explicit privileges");

        authzMap = core.authorize(AutoPutMappingAction.NAME, Sets.newHashSet("test_write2"), lookup, fieldPermissionsCache);
        assertThat(authzMap.get("test_write2").isGranted(), is(true));
        authzMap = core.authorize(PutMappingAction.NAME, Sets.newHashSet("test_write2"), lookup, fieldPermissionsCache);
        assertThat(authzMap.get("test_write2").isGranted(), is(false));
        authzMap = core.authorize(
                AutoPutMappingAction.NAME,
                Sets.newHashSet(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList())),
                lookup,
                fieldPermissionsCache);
        for (IndexMetadata im : backingIndices) {
            assertThat(authzMap.get(im.getIndex().getName()).isGranted(), is(true));
        }
        authzMap = core.authorize(
                PutMappingAction.NAME,
                Sets.newHashSet(backingIndices.stream().map(im -> im.getIndex().getName()).collect(Collectors.toList())),
                lookup,
                fieldPermissionsCache);
        for (IndexMetadata im : backingIndices) {
            assertThat(authzMap.get(im.getIndex().getName()).isGranted(), is(false));
        }
    }

    private static IndexMetadata createIndexMetadata(String name) {
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
