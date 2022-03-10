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
import java.util.function.Predicate;

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

    private static final Predicate<String> RESTRICTED_INDICES = name -> name != null && name.startsWith(".");

    public void testEmptyIndicesAccessControl() {
        IndicesAccessControl indicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap(), RESTRICTED_INDICES);
        assertTrue(indicesAccessControl.isGranted());
        assertNull(indicesAccessControl.getIndexPermissions(randomAlphaOfLengthBetween(3, 20)));
        assertThat(indicesAccessControl.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(indicesAccessControl.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(indicesAccessControl.getIndicesWithDocumentLevelSecurity(), emptyIterable());
    }

    public void testLimitedIndicesAccessControl() {
        IndicesAccessControl indicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap(), RESTRICTED_INDICES);
        IndicesAccessControl limitedByIndicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap(), RESTRICTED_INDICES);
        IndicesAccessControl result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.isGranted(), is(true));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        indicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap(), RESTRICTED_INDICES);
        limitedByIndicesAccessControl = new IndicesAccessControl(false, Collections.emptyMap(), RESTRICTED_INDICES);
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.isGranted(), is(false));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        indicesAccessControl = new IndicesAccessControl(false, Collections.emptyMap(), RESTRICTED_INDICES);
        limitedByIndicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap(), RESTRICTED_INDICES);
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.isGranted(), is(false));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        indicesAccessControl = new IndicesAccessControl(false, Collections.emptyMap(), RESTRICTED_INDICES);
        limitedByIndicesAccessControl = new IndicesAccessControl(false, Collections.emptyMap(), RESTRICTED_INDICES);
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.isGranted(), is(false));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        indicesAccessControl = new IndicesAccessControl(
            true,
            Collections.singletonMap("_index", new IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll())),
            RESTRICTED_INDICES
        );
        limitedByIndicesAccessControl = new IndicesAccessControl(true, Collections.emptyMap(), RESTRICTED_INDICES);
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.getIndexPermissions("_index"), is(nullValue()));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        indicesAccessControl = new IndicesAccessControl(
            true,
            Collections.singletonMap("_index", new IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll())),
            RESTRICTED_INDICES
        );
        limitedByIndicesAccessControl = new IndicesAccessControl(
            true,
            Collections.singletonMap("_index", new IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll())),
            RESTRICTED_INDICES
        );
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(result.getIndexPermissions("_index").isGranted(), is(true));
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
                Map.entry("_index", new IndexAccessControl(true, fieldPermissions1, DocumentPermissions.allowAll())),
                Map.entry("another-index", new IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll()))
            ),
            RESTRICTED_INDICES
        );
        limitedByIndicesAccessControl = new IndicesAccessControl(
            true,
            Map.ofEntries(
                Map.entry("_index", new IndexAccessControl(true, fieldPermissions2, DocumentPermissions.allowAll())),
                Map.entry("another-index", new IndexAccessControl(true, fieldPermissions2, DocumentPermissions.allowAll()))
            ),
            RESTRICTED_INDICES
        );
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(result.getIndexPermissions("_index").isGranted(), is(true));
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
        assertThat(documentPermissions1.getQueries(), equalTo(queries));

        final DocumentPermissions documentPermissions2 = DocumentPermissions.filteredBy(
            Set.of(new BytesArray("{\"term\":{ \"public\":true } }"))
        );

        indicesAccessControl = new IndicesAccessControl(
            true,
            Map.ofEntries(
                Map.entry("_index", new IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll())),
                Map.entry("another-index", new IndexAccessControl(true, new FieldPermissions(), documentPermissions2))
            ),
            RESTRICTED_INDICES
        );
        limitedByIndicesAccessControl = new IndicesAccessControl(
            true,
            Map.ofEntries(
                Map.entry("_index", new IndexAccessControl(true, new FieldPermissions(), documentPermissions1)),
                Map.entry("another-index", new IndexAccessControl(true, new FieldPermissions(), DocumentPermissions.allowAll()))
            ),
            RESTRICTED_INDICES
        );
        result = indicesAccessControl.limitIndicesAccessControl(limitedByIndicesAccessControl);
        assertThat(result, is(notNullValue()));
        assertThat(result.getIndexPermissions("_index"), is(notNullValue()));
        assertThat(result.getIndexPermissions("_index").isGranted(), is(true));
        assertThat(result.getIndexPermissions("_index").getFieldPermissions().hasFieldLevelSecurity(), is(false));
        assertThat(result.getIndexPermissions("_index").getDocumentPermissions().hasDocumentLevelPermissions(), is(true));
        assertThat(result.getIndexPermissions("_index").getDocumentPermissions().getQueries(), is(nullValue()));
        assertThat(result.getIndexPermissions("_index").getDocumentPermissions().getLimitedByQueries(), equalTo(queries));
        assertThat(result.getIndicesWithFieldOrDocumentLevelSecurity(), containsInAnyOrder("_index", "another-index"));
        assertThat(result.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(result.getIndicesWithDocumentLevelSecurity(), containsInAnyOrder("_index", "another-index"));
    }

    public void testAllowAllIndicesAccessControl() {
        final IndicesAccessControl allowAll = IndicesAccessControl.allowAll();
        final IndexAccessControl indexAccessControl = allowAll.getIndexPermissions(randomAlphaOfLengthBetween(3, 8));
        assertThat(indexAccessControl.isGranted(), is(true));
        assertThat(indexAccessControl.getDocumentPermissions(), is(DocumentPermissions.allowAll()));
        assertThat(indexAccessControl.getFieldPermissions(), is(FieldPermissions.DEFAULT));

        final IndicesAccessControl.DeniedIndices deniedIndices = allowAll.getDeniedIndices();
        assertThat(deniedIndices.regularIndices(), emptyIterable());
        assertThat(deniedIndices.restrictedIndices(), emptyIterable());

        assertThat(allowAll.getFieldAndDocumentLevelSecurityUsage(), is(IndicesAccessControl.DlsFlsUsage.NONE));
        assertThat(allowAll.getIndicesWithFieldOrDocumentLevelSecurity(), emptyIterable());
        assertThat(allowAll.getIndicesWithFieldLevelSecurity(), emptyIterable());
        assertThat(allowAll.getIndicesWithDocumentLevelSecurity(), emptyIterable());

        final IndicesAccessControl indicesAccessControl = new IndicesAccessControl(randomBoolean(), Map.of(), RESTRICTED_INDICES);
        assertThat(allowAll.limitIndicesAccessControl(indicesAccessControl), is(indicesAccessControl));
        assertThat(indicesAccessControl.limitIndicesAccessControl(allowAll), is(indicesAccessControl));
    }

    public void testSplittingDeniedIndicesByRestrictedFlag() {
        final IndicesAccessControl iac = new IndicesAccessControl(
            false,
            Map.ofEntries(
                Map.entry("denied-index-1", new IndexAccessControl(false, FieldPermissions.DEFAULT, DocumentPermissions.allowAll())),
                Map.entry("allowed-index-1", new IndexAccessControl(true, FieldPermissions.DEFAULT, DocumentPermissions.allowAll())),
                Map.entry(".restricted-index-1", new IndexAccessControl(false, FieldPermissions.DEFAULT, DocumentPermissions.allowAll())),
                Map.entry("denied-index-2", new IndexAccessControl(false, FieldPermissions.DEFAULT, DocumentPermissions.allowAll())),
                Map.entry("allowed-index-2", new IndexAccessControl(true, FieldPermissions.DEFAULT, DocumentPermissions.allowAll())),
                Map.entry(".restricted-index-2", new IndexAccessControl(false, FieldPermissions.DEFAULT, DocumentPermissions.allowAll())),
                Map.entry("denied-index-3", new IndexAccessControl(false, FieldPermissions.DEFAULT, DocumentPermissions.allowAll()))
            ),
            RESTRICTED_INDICES
        );
        final IndicesAccessControl.DeniedIndices deniedIndices = iac.getDeniedIndices();
        assertThat(deniedIndices.regularIndices(), containsInAnyOrder("denied-index-1", "denied-index-2", "denied-index-3"));
        assertThat(deniedIndices.restrictedIndices(), containsInAnyOrder(".restricted-index-1", ".restricted-index-2"));
        assertThat(deniedIndices.isEmpty(), is(false));
    }
}
