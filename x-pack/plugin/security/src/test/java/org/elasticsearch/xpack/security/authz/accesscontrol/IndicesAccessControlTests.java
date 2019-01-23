/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;

import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl.IndexAccessControl;

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
    }
}
