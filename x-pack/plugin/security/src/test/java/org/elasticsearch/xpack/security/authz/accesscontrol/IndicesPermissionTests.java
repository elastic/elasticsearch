/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IndicesPermissionTests extends ESTestCase {

    public void testAuthorize() {
        IndexMetaData.Builder imbBuilder = IndexMetaData.builder("_index")
                .settings(Settings.builder()
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                )
                .putAlias(AliasMetaData.builder("_alias"));
        MetaData md = MetaData.builder().put(imbBuilder).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);

        // basics:
        Set<BytesReference> query = Collections.singleton(new BytesArray("{}"));
        String[] fields = new String[]{"_field"};
        Role role = Role.builder("_role")
                .add(new FieldPermissions(fieldPermissionDef(fields, null)), query, IndexPrivilege.ALL, "_index").build();
        IndicesAccessControl permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_index"), md, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getQueries().size(), equalTo(1));
        assertThat(permissions.getIndexPermissions("_index").getQueries(), equalTo(query));

        // no document level security:
        role = Role.builder("_role")
                .add(new FieldPermissions(fieldPermissionDef(fields, null)), null, IndexPrivilege.ALL, "_index").build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_index"), md, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getQueries(), nullValue());

        // no field level security:
        role = Role.builder("_role").add(new FieldPermissions(), query, IndexPrivilege.ALL, "_index").build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_index"), md, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getQueries().size(), equalTo(1));
        assertThat(permissions.getIndexPermissions("_index").getQueries(), equalTo(query));

        // index group associated with an alias:
        role = Role.builder("_role")
                .add(new FieldPermissions(fieldPermissionDef(fields, null)), query, IndexPrivilege.ALL, "_alias")
                .build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_alias"), md, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getQueries().size(), equalTo(1));
        assertThat(permissions.getIndexPermissions("_index").getQueries(), equalTo(query));

        assertThat(permissions.getIndexPermissions("_alias"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_alias").getFieldPermissions().grantsAccessTo("_field"));
        assertTrue(permissions.getIndexPermissions("_alias").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_alias").getQueries().size(), equalTo(1));
        assertThat(permissions.getIndexPermissions("_alias").getQueries(), equalTo(query));

        // match all fields
        String[] allFields = randomFrom(new String[]{"*"}, new String[]{"foo", "*"},
        new String[]{randomAlphaOfLengthBetween(1, 10), "*"});
        role = Role.builder("_role")
                .add(new FieldPermissions(fieldPermissionDef(allFields, null)), query, IndexPrivilege.ALL, "_alias").build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_alias"), md, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getQueries().size(), equalTo(1));
        assertThat(permissions.getIndexPermissions("_index").getQueries(), equalTo(query));

        assertThat(permissions.getIndexPermissions("_alias"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_alias").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_alias").getQueries().size(), equalTo(1));
        assertThat(permissions.getIndexPermissions("_alias").getQueries(), equalTo(query));

        IndexMetaData.Builder imbBuilder1 = IndexMetaData.builder("_index_1")
                .settings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                )
                .putAlias(AliasMetaData.builder("_alias"));
        md = MetaData.builder(md).put(imbBuilder1).build();


        // match all fields with more than one permission
        Set<BytesReference> fooQuery = Collections.singleton(new BytesArray("{foo}"));
        allFields = randomFrom(new String[]{"*"}, new String[]{"foo", "*"},
                new String[]{randomAlphaOfLengthBetween(1, 10), "*"});
        role = Role.builder("_role")
                .add(new FieldPermissions(fieldPermissionDef(allFields, null)), fooQuery, IndexPrivilege.ALL, "_alias")
                .add(new FieldPermissions(fieldPermissionDef(allFields, null)), query, IndexPrivilege.ALL, "_alias").build();
        permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_alias"), md, fieldPermissionsCache);
        Set<BytesReference> bothQueries = Sets.union(fooQuery, query);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index").getQueries().size(), equalTo(2));
        assertThat(permissions.getIndexPermissions("_index").getQueries(), equalTo(bothQueries));

        assertThat(permissions.getIndexPermissions("_index_1"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_index_1").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_index_1").getQueries().size(), equalTo(2));
        assertThat(permissions.getIndexPermissions("_index_1").getQueries(), equalTo(bothQueries));

        assertThat(permissions.getIndexPermissions("_alias"), notNullValue());
        assertFalse(permissions.getIndexPermissions("_alias").getFieldPermissions().hasFieldLevelSecurity());
        assertThat(permissions.getIndexPermissions("_alias").getQueries().size(), equalTo(2));
        assertThat(permissions.getIndexPermissions("_alias").getQueries(), equalTo(bothQueries));

    }

    public void testAuthorizeMultipleGroupsMixedDls() {
        IndexMetaData.Builder imbBuilder = IndexMetaData.builder("_index")
                .settings(Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                )
                .putAlias(AliasMetaData.builder("_alias"));
        MetaData md = MetaData.builder().put(imbBuilder).build();
        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);

        Set<BytesReference> query = Collections.singleton(new BytesArray("{}"));
        String[] fields = new String[]{"_field"};
        Role role = Role.builder("_role")
                .add(new FieldPermissions(fieldPermissionDef(fields, null)), query, IndexPrivilege.ALL, "_index")
                .add(new FieldPermissions(fieldPermissionDef(null, null)), null, IndexPrivilege.ALL, "*")
                .build();
        IndicesAccessControl permissions = role.authorize(SearchAction.NAME, Sets.newHashSet("_index"), md, fieldPermissionsCache);
        assertThat(permissions.getIndexPermissions("_index"), notNullValue());
        assertTrue(permissions.getIndexPermissions("_index").getFieldPermissions().grantsAccessTo("_field"));
        assertFalse(permissions.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity());
        // null implies that there is no DLS. Currently a index permissions only has queries defined
        // on it and not a true document level permission object like fields
        assertNull(permissions.getIndexPermissions("_index").getQueries());
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
        out.setVersion(Version.V_6_0_0);
        indicesPrivileges = RoleDescriptor.IndicesPrivileges.builder();
        indicesPrivileges.grantedFields(allowed);
        indicesPrivileges.deniedFields(denied);
        indicesPrivileges.query("{match_all:{}}");
        indicesPrivileges.indices(readIndicesPrivileges.getIndices());
        indicesPrivileges.privileges("all", "read", "priv");
        indicesPrivileges.build().writeTo(out);
        out.close();
        in = out.bytes().streamInput();
        in.setVersion(Version.V_6_0_0);
        RoleDescriptor.IndicesPrivileges readIndicesPrivileges2 = new RoleDescriptor.IndicesPrivileges(in);
        assertEquals(readIndicesPrivileges, readIndicesPrivileges2);
    }

    // tests that field permissions are merged correctly when we authorize with several groups and don't crash when an index has no group
    public void testCorePermissionAuthorize() {
        final Settings indexSettings = Settings.builder().put("index.version.created", Version.CURRENT).build();
        final MetaData metaData = new MetaData.Builder()
                .put(new IndexMetaData.Builder("a1").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .put(new IndexMetaData.Builder("a2").settings(indexSettings).numberOfShards(1).numberOfReplicas(0).build(), true)
                .build();

        FieldPermissionsCache fieldPermissionsCache = new FieldPermissionsCache(Settings.EMPTY);
        IndicesPermission.Group group1 = new IndicesPermission.Group(IndexPrivilege.ALL, new FieldPermissions(), null, "a1");
        IndicesPermission.Group group2 = new IndicesPermission.Group(IndexPrivilege.ALL,
                new FieldPermissions(fieldPermissionDef(null, new String[]{"denied_field"})), null, "a1");
        IndicesPermission core = new IndicesPermission(group1, group2);
        Map<String, IndicesAccessControl.IndexAccessControl> authzMap =
                core.authorize(SearchAction.NAME, Sets.newHashSet("a1", "ba"), metaData, fieldPermissionsCache);
        assertTrue(authzMap.get("a1").getFieldPermissions().grantsAccessTo("denied_field"));
        assertTrue(authzMap.get("a1").getFieldPermissions().grantsAccessTo(randomAlphaOfLength(5)));
        // did not define anything for ba so we allow all
        assertFalse(authzMap.get("ba").getFieldPermissions().hasFieldLevelSecurity());

        assertTrue(core.check(SearchAction.NAME));
        assertFalse(core.check("unknown"));

        // test with two indices
        group1 = new IndicesPermission.Group(IndexPrivilege.ALL, new FieldPermissions(), null, "a1");
        group2 = new IndicesPermission.Group(IndexPrivilege.ALL,
                new FieldPermissions(fieldPermissionDef(null, new String[]{"denied_field"})), null, "a1");
        IndicesPermission.Group group3 = new IndicesPermission.Group(IndexPrivilege.ALL,
                new FieldPermissions(fieldPermissionDef(new String[]{"*_field"}, new String[]{"denied_field"})), null, "a2");
        IndicesPermission.Group group4 = new IndicesPermission.Group(IndexPrivilege.ALL,
                new FieldPermissions(fieldPermissionDef(new String[]{"*_field2"}, new String[]{"denied_field2"})), null, "a2");
        core = new IndicesPermission(group1, group2, group3, group4);
        authzMap = core.authorize(SearchAction.NAME, Sets.newHashSet("a1", "a2"), metaData, fieldPermissionsCache);
        assertFalse(authzMap.get("a1").getFieldPermissions().hasFieldLevelSecurity());
        assertFalse(authzMap.get("a2").getFieldPermissions().grantsAccessTo("denied_field2"));
        assertFalse(authzMap.get("a2").getFieldPermissions().grantsAccessTo("denied_field"));
        assertTrue(authzMap.get("a2").getFieldPermissions().grantsAccessTo(randomAlphaOfLength(5) + "_field"));
        assertTrue(authzMap.get("a2").getFieldPermissions().grantsAccessTo(randomAlphaOfLength(5) + "_field2"));
        assertTrue(authzMap.get("a2").getFieldPermissions().hasFieldLevelSecurity());

        assertTrue(core.check(SearchAction.NAME));
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
                () -> new IndicesPermission.Group(IndexPrivilege.ALL, new FieldPermissions(), null, indices.toArray(Strings.EMPTY_ARRAY)));
        assertThat(e.getMessage(), containsString(indices.get(0)));
        assertThat(e.getMessage(), containsString("too complex to evaluate"));
    }

    private static FieldPermissionsDefinition fieldPermissionDef(String[] granted, String[] denied) {
        return new FieldPermissionsDefinition(granted, denied);
    }
}
