/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.elasticsearch.client.internal.Client;
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
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

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

        final Set<BytesReference> queries = Collections.singleton(new BytesArray("{\"match_all\" : {}}"));
        final DocumentPermissions documentPermissions2 = DocumentPermissions.filteredBy(queries);
        assertThat(documentPermissions2, is(notNullValue()));
        assertThat(documentPermissions2.hasDocumentLevelPermissions(), is(true));
        assertThat(documentPermissions2.getListOfQueries(), equalTo(List.of(queries)));

        final DocumentPermissions documentPermissions3 = documentPermissions1.limitDocumentPermissions(documentPermissions2);
        assertThat(documentPermissions3, is(notNullValue()));
        assertThat(documentPermissions3.hasDocumentLevelPermissions(), is(true));
        assertThat(documentPermissions3.getListOfQueries(), equalTo(List.of(queries)));

        final DocumentPermissions documentPermissions4 = DocumentPermissions.allowAll()
            .limitDocumentPermissions(DocumentPermissions.allowAll());
        assertThat(documentPermissions4, is(notNullValue()));
        assertThat(documentPermissions4.hasDocumentLevelPermissions(), is(false));

        final DocumentPermissions documentPermissions5 = DocumentPermissions.allowAll().limitDocumentPermissions(documentPermissions3);
        assertThat(documentPermissions5, is(notNullValue()));
        assertThat(documentPermissions5.hasDocumentLevelPermissions(), is(true));
        assertThat(documentPermissions5.getListOfQueries(), equalTo(List.of(queries)));
    }

    public void testMultipleSetsOfQueries() {
        final Set<BytesReference> queries = Collections.singleton(new BytesArray("{\"match_all\" : {}}"));
        DocumentPermissions documentPermissions = DocumentPermissions.allowAll();
        final int nSets = randomIntBetween(2, 8);
        for (int i = 0; i < nSets; i++) {
            documentPermissions = documentPermissions.limitDocumentPermissions(DocumentPermissions.filteredBy(queries));
        }

        assertThat(documentPermissions.hasDocumentLevelPermissions(), is(true));
        assertThat(documentPermissions.getListOfQueries(), equalTo(IntStream.range(0, nSets).mapToObj(i -> queries).toList()));
    }

    public void testFailIfQueryUsesClient() throws Exception {
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        final long nowInMillis = randomNonNegativeLong();
        QueryRewriteContext context = new QueryRewriteContext(parserConfig(), client, () -> nowInMillis);
        QueryBuilder queryBuilder1 = new TermsQueryBuilder("field", "val1", "val2");
        DocumentPermissions.failIfQueryUsesClient(queryBuilder1, context);

        QueryBuilder queryBuilder2 = new TermsQueryBuilder("field", new TermsLookup("_index", "_id", "_path"));
        Exception e = expectThrows(IllegalStateException.class, () -> DocumentPermissions.failIfQueryUsesClient(queryBuilder2, context));
        assertThat(e.getMessage(), equalTo("role queries are not allowed to execute additional requests"));
    }

    public void testWriteCacheKeyWillDistinguishBetweenQueriesAndLimitedByQueries() throws IOException {
        final BytesStreamOutput out0 = new BytesStreamOutput();
        final DocumentPermissions documentPermissions0 = DocumentPermissions.filteredBy(
            Set.of(
                new BytesArray("{\"term\":{\"q1\":\"v1\"}}"),
                new BytesArray("{\"term\":{\"q2\":\"v2\"}}"),
                new BytesArray("{\"term\":{\"q3\":\"v3\"}}")
            )
        );
        documentPermissions0.buildCacheKey(out0, BytesReference::utf8ToString);

        final BytesStreamOutput out1 = new BytesStreamOutput();
        final DocumentPermissions documentPermissions1 = DocumentPermissions.filteredBy(
            Set.of(new BytesArray("{\"term\":{\"q1\":\"v1\"}}"), new BytesArray("{\"term\":{\"q2\":\"v2\"}}"))
        ).limitDocumentPermissions(DocumentPermissions.filteredBy(Set.of(new BytesArray("{\"term\":{\"q3\":\"v3\"}}"))));
        documentPermissions1.buildCacheKey(out1, BytesReference::utf8ToString);

        final BytesStreamOutput out2 = new BytesStreamOutput();
        final DocumentPermissions documentPermissions2 = DocumentPermissions.filteredBy(
            Set.of(new BytesArray("{\"term\":{\"q1\":\"v1\"}}"))
        )
            .limitDocumentPermissions(
                DocumentPermissions.filteredBy(
                    Set.of(new BytesArray("{\"term\":{\"q2\":\"v2\"}}"), new BytesArray("{\"term\":{\"q3\":\"v3\"}}"))
                )
            );
        documentPermissions2.buildCacheKey(out2, BytesReference::utf8ToString);

        assertThat(Arrays.equals(BytesReference.toBytes(out0.bytes()), BytesReference.toBytes(out1.bytes())), is(false));
        assertThat(Arrays.equals(BytesReference.toBytes(out0.bytes()), BytesReference.toBytes(out2.bytes())), is(false));
        assertThat(Arrays.equals(BytesReference.toBytes(out1.bytes()), BytesReference.toBytes(out2.bytes())), is(false));
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
        if (queries.isEmpty() || randomBoolean()) {
            queries.add(new BytesArray("{\"term\":{\"tag\":\"prod\"}}"));
        }
        final DocumentPermissions documentPermissions0 = DocumentPermissions.filteredBy(queries);
        assertThat(documentPermissions0.hasStoredScript(), is(hasStoredScript));
    }
}
