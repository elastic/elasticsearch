/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
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
        assertNull(indicesAccessControl.getIndexPermissions(randomAlphaOfLengthBetween(3, 20)));
        assertThat(indicesAccessControl.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(indicesAccessControl.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(indicesAccessControl.getIndicesWithDocumentLevelSecurity(), emptyIterable());
    }

    public void testLimitedIndicesAccessControl() {
        IndicesAccessControl indicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        IndicesAccessControl limitedByIndicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        IndicesAccessControl result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.isGranted(), is(true));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        indicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        limitedByIndicesAccessControl = new IndicesAccessControl(false, Collections.emptyMap());
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.isGranted(), is(false));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        indicesAccessControl = new IndicesAccessControl(false, Collections.emptyMap());
        limitedByIndicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.isGranted(), is(false));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        indicesAccessControl = new IndicesAccessControl(false, Collections.emptyMap());
        limitedByIndicesAccessControl = new IndicesAccessControl(false, Collections.emptyMap());
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.isGranted(), is(false));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        indicesAccessControl = new IndicesAccessControl(
            true,
            Collections.singletonMap("_index", new IndexAccessControl(FieldPermissions.DEFAULT, DocumentPermissions.allowAll()))
        );
        limitedByIndicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap());
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        indicesAccessControl = new IndicesAccessControl(
            true,
            Collections.singletonMap("_index", new IndexAccessControl(FieldPermissions.DEFAULT, DocumentPermissions.allowAll()))
        );
        limitedByIndicesAccessControl = new IndicesAccessControl(
            true,
            Collections.singletonMap("_index", new IndexAccessControl(FieldPermissions.DEFAULT, DocumentPermissions.allowAll()))
        );
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(result.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity(), is(false));
        assertThat(result.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(false));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        final FieldPermissions fieldPermissions1 = new FieldPermissions(
            new FieldPermissionsDefinition(new String[] { "f1", "f2", "f3*" }, new String[] { "f3" })
        );
        final FieldPermissions fieldPermissions2 = new FieldPermissions(
            new FieldPermissionsDefinition(new String[] { "f1", "f3*", "f4" }, new String[] { "f3" })
        );
        indicesAccessControl = new IndicesAccessControl(
            true,
            Map.ofEntries(
                Map.entry("_index", new IndexAccessControl(fieldPermissions1, DocumentPermissions.allowAll())),
                Map.entry("another-index", new IndexAccessControl(FieldPermissions.DEFAULT, DocumentPermissions.allowAll()))
            )
        );
        limitedByIndicesAccessControl = new IndicesAccessControl(
            true,
            Map.ofEntries(
                Map.entry("_index", new IndexAccessControl(fieldPermissions2, DocumentPermissions.allowAll())),
                Map.entry("another-index", new IndexAccessControl(fieldPermissions2, DocumentPermissions.allowAll()))
            )
        );
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(result.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity(), is(true));
        assertThat(result.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(false));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), containsInAnyOrder("_index", "another-index"));
        assertThat(result.getIndicesWithFieldLevelSecurity(), containsInAnyOrder("_index", "another-index"));
        assertThat(result.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        FieldPermissions resultFieldPermissions = result.getIndexPermissions("_index").getFieldPermissions();
        assertThat(resultFieldPermissions.grantsAccessTo("f1"), is(true));
        assertThat(resultFieldPermissions.grantsAccessTo("f2"), is(false));
        assertThat(resultFieldPermissions.grantsAccessTo("f3"), is(false));
        assertThat(resultFieldPermissions.grantsAccessTo("f31"), is(true));
        assertThat(resultFieldPermissions.grantsAccessTo("f4"), is(false));

        Set<BytesReference> queries = Collections.singleton(new BytesArray("{\"match_all\" : {}}"));
        final DocumentPermissions documentPermissions1 = DocumentPermissions.filteredBy(queries);
        assertThat(documentPermissions1, is(notNullValue()));
        assertThat(documentPermissions1.hasDocumentLevelPermissions(), is(true));
        assertThat(documentPermissions1.getSingleSetOfQueries(), equalTo(queries));

        final DocumentPermissions documentPermissions2 = DocumentPermissions.filteredBy(
            Set.of(new BytesArray("{\"term\":{ \"public\":true } }"))
        );

        indicesAccessControl = new IndicesAccessControl(
            true,
            Map.ofEntries(
                Map.entry("_index", new IndexAccessControl(FieldPermissions.DEFAULT, DocumentPermissions.allowAll())),
                Map.entry("another-index", new IndexAccessControl(FieldPermissions.DEFAULT, documentPermissions2))
            )
        );
        limitedByIndicesAccessControl = new IndicesAccessControl(
            true,
            Map.ofEntries(
                Map.entry("_index", new IndexAccessControl(FieldPermissions.DEFAULT, documentPermissions1)),
                Map.entry("another-index", new IndexAccessControl(FieldPermissions.DEFAULT, DocumentPermissions.allowAll()))
            )
        );
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(result.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity(), is(false));
        assertThat(result.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(result.getIndexPermissions("_index").getDocumentPermissions().getSingleSetOfQueries(), equalTo(queries));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), containsInAnyOrder("_index", "another-index"));
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), containsInAnyOrder("_index", "another-index"));
    }

    public void testAllowAllIndicesAccessControl() {
        final IndicesAccessControl allowAll = IndicesAccessControl.allowAll();
        final IndexAccessControl indexAccessControl = allowAll.getIndexPermissions(randomAlphaOfLengthBetween(3, 8));
        assertThat(indexAccessControl.getDocumentPermissions(), is(DocumentPermissions.allowAll()));
        assertThat(indexAccessControl.getFieldPermissions(), is(FieldPermissions.DEFAULT));
        assertThat(allowAll.hasIndexPermissions(randomAlphaOfLengthBetween(3, 8)), is(true));
        assertThat(allowAll.getFieldAndDocumentLevelSecurityUsage(), is(IndicesAccessControl.DlsFlsUsage.NONE));
        assertThat(allowAll.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(allowAll.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(allowAll.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        final IndicesAccessControl indicesAccessControl = new IndicesAccessControl(randomBoolean(), Map.of());
        assertThat(allowAll.limitIndicesAccessControl(indicesAccessControl), is(indicesAccessControl));
        assertThat(indicesAccessControl.limitIndicesAccessControl(allowAll), is(indicesAccessControl));
    }
}
