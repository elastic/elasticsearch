/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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

    public void testWriteCacheKeyWillDistinguishBetweenQueriesAndLimitedByQueries() throws IOException {
        final BytesStreamOutput out0 = new BytesStreamOutput();
        final DocumentPermissions documentPermissions0 =
            new DocumentPermissions(
                Set.of(new BytesArray("{\"term\":{\"q1\":\"v1\"}}"),
                    new BytesArray("{\"term\":{\"q2\":\"v2\"}}"), new BytesArray("{\"term\":{\"q3\":\"v3\"}}")),
                null);
        documentPermissions0.buildCacheKey(out0, BytesReference::utf8ToString);

        final BytesStreamOutput out1 = new BytesStreamOutput();
        final DocumentPermissions documentPermissions1 =
            new DocumentPermissions(
                Set.of(new BytesArray("{\"term\":{\"q1\":\"v1\"}}"), new BytesArray("{\"term\":{\"q2\":\"v2\"}}")),
                Set.of(new BytesArray("{\"term\":{\"q3\":\"v3\"}}")));
        documentPermissions1.buildCacheKey(out1, BytesReference::utf8ToString);

        final BytesStreamOutput out2 = new BytesStreamOutput();
        final DocumentPermissions documentPermissions2 =
            new DocumentPermissions(
                Set.of(new BytesArray("{\"term\":{\"q1\":\"v1\"}}")),
                Set.of(new BytesArray("{\"term\":{\"q2\":\"v2\"}}"), new BytesArray("{\"term\":{\"q3\":\"v3\"}}")));
        documentPermissions2.buildCacheKey(out2, BytesReference::utf8ToString);

        final BytesStreamOutput out3 = new BytesStreamOutput();
        final DocumentPermissions documentPermissions3 =
            new DocumentPermissions(
                null,
                Set.of(new BytesArray("{\"term\":{\"q1\":\"v1\"}}"),
                    new BytesArray("{\"term\":{\"q2\":\"v2\"}}"), new BytesArray("{\"term\":{\"q3\":\"v3\"}}")));
        documentPermissions3.buildCacheKey(out3, BytesReference::utf8ToString);

        assertThat(Arrays.equals(BytesReference.toBytes(out0.bytes()), BytesReference.toBytes(out1.bytes())), is(false));
        assertThat(Arrays.equals(BytesReference.toBytes(out0.bytes()), BytesReference.toBytes(out2.bytes())), is(false));
        assertThat(Arrays.equals(BytesReference.toBytes(out0.bytes()), BytesReference.toBytes(out3.bytes())), is(false));
        assertThat(Arrays.equals(BytesReference.toBytes(out1.bytes()), BytesReference.toBytes(out2.bytes())), is(false));
        assertThat(Arrays.equals(BytesReference.toBytes(out1.bytes()), BytesReference.toBytes(out3.bytes())), is(false));
        assertThat(Arrays.equals(BytesReference.toBytes(out2.bytes()), BytesReference.toBytes(out3.bytes())), is(false));
    }

    public void testHasStoredScript() throws IOException {
        final Set<BytesReference> queries = new HashSet<>();
        if (randomBoolean()) {
            queries.add(new BytesArray("{\"term\":{\"username\":\"foo\"}}"));
        }
        final boolean hasStoredScript = randomBoolean();
        if (hasStoredScript) {
            queries.add(new BytesArray("{\"template\":{\"id\":\"my-script\"}}"));
        }
        final DocumentPermissions documentPermissions0 =
            randomBoolean() ? new DocumentPermissions(queries, null) : new DocumentPermissions(null, queries);
        assertThat(documentPermissions0.hasStoredScript(), is(hasStoredScript));
    }
}
