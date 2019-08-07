/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl.IndexAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link IndicesAccessControl}
 */
public class IndicesAccessControlTests extends ESTestCase {

    public void testEmptyIndicesAccessControl() {
        IndicesAccessControl indicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        assertTrue(indicesAccessControl.isGranted());
        assertNull(indicesAccessControl.getIndexPermissions(randomAlphaOfLengthBetween(3,20)));
    }

    public void testSLimitedIndicesAccessControl() {
        IndicesAccessControl indicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        IndicesAccessControl limitedByIndicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        IndicesAccessControl result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.isGranted(), is(true));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));

        indicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        limitedByIndicesAccessControl = new IndicesAccessControl(false, Collections.emptyMap());
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.isGranted(), is(false));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));

        indicesAccessControl = new IndicesAccessControl(false, Collections.emptyMap());
        limitedByIndicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.isGranted(), is(false));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));

        indicesAccessControl = new IndicesAccessControl(false, Collections.emptyMap());
        limitedByIndicesAccessControl = new IndicesAccessControl(false, Collections.emptyMap());
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.isGranted(), is(false));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));

        indicesAccessControl = new IndicesAccessControl(true,
                Collections.singletonMap("_index", new IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll())));
        limitedByIndicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));

        indicesAccessControl = new IndicesAccessControl(true,
                Collections.singletonMap("_index", new IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll())));
        limitedByIndicesAccessControl = new IndicesAccessControl(true,
                Collections.singletonMap("_index", new IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll())));
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(result.getIndexPermissions("_index").isGranted(), is(true));
        assertThat(result.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity(), is(false));
        assertThat(result.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(false));

        final FieldPermissions fieldPermissions1 = new FieldPermissions(
                new FieldPermissionsDefinition(new String[] { "f1", "f2", "f3*" }, new String[] { "f3" }));
        final FieldPermissions fieldPermissions2 = new FieldPermissions(
                new FieldPermissionsDefinition(new String[] { "f1", "f3*", "f4" }, new String[] { "f3" }));
        indicesAccessControl = new IndicesAccessControl(true,
                Collections.singletonMap("_index", new IndexAccessControl(true, fieldPermissions1, DocumentPermissions.allowAll())));
        limitedByIndicesAccessControl = new IndicesAccessControl(true,
                Collections.singletonMap("_index", new IndexAccessControl(true, fieldPermissions2, DocumentPermissions.allowAll())));
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(result.getIndexPermissions("_index").isGranted(), is(true));
        assertThat(result.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity(), is(true));
        assertThat(result.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(false));
        FieldPermissions resultFieldPermissions = result.getIndexPermissions("_index").getFieldPermissions();
        assertThat(resultFieldPermissions.grantsAccessTo("f1"), is(true));
        assertThat(resultFieldPermissions.grantsAccessTo("f2"), is(false));
        assertThat(resultFieldPermissions.grantsAccessTo("f3"), is(false));
        assertThat(resultFieldPermissions.grantsAccessTo("f31"), is(true));
        assertThat(resultFieldPermissions.grantsAccessTo("f4"), is(false));

        Set<BytesReference> queries = Collections.singleton(new BytesArray("{\"match_all\" : {}}"));
        final DocumentPermissions documentPermissions = DocumentPermissions
                .filteredBy(queries);
        assertThat(documentPermissions, is(notNullValue()));
        assertThat(documentPermissions.hasDocumentLevelPermissions(), is(true));
        assertThat(documentPermissions.getQueries(), equalTo(queries));

        indicesAccessControl = new IndicesAccessControl(true,
                Collections.singletonMap("_index", new IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll())));
        limitedByIndicesAccessControl = new IndicesAccessControl(true,
                Collections.singletonMap("_index", new IndexAccessControl(true, new FieldPermissions(), documentPermissions)));
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(result.getIndexPermissions("_index").isGranted(), is(true));
        assertThat(result.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity(), is(false));
        assertThat(result.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(result.getIndexPermissions("_index").getDocumentPermissions().getQueries(), is(nullValue()));
        assertThat(result.getIndexPermissions("_index").getDocumentPermissions().getLimitedByQueries(), equalTo(queries));
    }
}
