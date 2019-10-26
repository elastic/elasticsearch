/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DocumentPermissionsTests extends ESTestCase {

    public void testHasDocumentPermissions() throws IOException {
        final DocumentPermissions documentPermissions1 = DocumentPermissions.allowAll();
        assertThat(documentPermissions1, is(notNullValue()));
        assertThat(documentPermissions1.hasDocumentLevelPermissions(), is(false));
        assertThat(documentPermissions1.filter(null, null, null, null), is(nullValue()));

        Set<BytesReference> queries = Collections.singleton(new BytesArray("{\"match_all\" : {}}"));
        final DocumentPermissions documentPermissions2 = DocumentPermissions
                .filteredBy(queries);
        assertThat(documentPermissions2, is(notNullValue()));
        assertThat(documentPermissions2.hasDocumentLevelPermissions(), is(true));
        assertThat(documentPermissions2.getQueries(), equalTo(queries));

        final DocumentPermissions documentPermissions3 = documentPermissions1.limitDocumentPermissions(documentPermissions2);
        assertThat(documentPermissions3, is(notNullValue()));
        assertThat(documentPermissions3.hasDocumentLevelPermissions(), is(true));
        assertThat(documentPermissions3.getQueries(), is(nullValue()));
        assertThat(documentPermissions3.getLimitedByQueries(), equalTo(queries));

        final DocumentPermissions documentPermissions4 = DocumentPermissions.allowAll()
                .limitDocumentPermissions(DocumentPermissions.allowAll());
        assertThat(documentPermissions4, is(notNullValue()));
        assertThat(documentPermissions4.hasDocumentLevelPermissions(), is(false));

        AssertionError ae = expectThrows(AssertionError.class,
                () -> DocumentPermissions.allowAll().limitDocumentPermissions(documentPermissions3));
        assertThat(ae.getMessage(), containsString("nested scoping for document permissions is not permitted"));
    }

    public void testFailIfQueryUsesClient() throws Exception {
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        final long nowInMillis = randomNonNegativeLong();
        QueryRewriteContext context = new QueryRewriteContext(xContentRegistry(), writableRegistry(), client,
                () -> nowInMillis);
        QueryBuilder queryBuilder1 = new TermsQueryBuilder("field", "val1", "val2");
        DocumentPermissions.failIfQueryUsesClient(queryBuilder1, context);

        QueryBuilder queryBuilder2 = new TermsQueryBuilder("field", new TermsLookup("_index", "_id", "_path"));
        Exception e = expectThrows(IllegalStateException.class,
                () -> DocumentPermissions.failIfQueryUsesClient(queryBuilder2, context));
        assertThat(e.getMessage(), equalTo("role queries are not allowed to execute additional requests"));
    }
}
