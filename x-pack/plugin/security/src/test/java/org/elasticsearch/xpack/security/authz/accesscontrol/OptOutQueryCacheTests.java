/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** Simple tests for opt out query cache*/
public class OptOutQueryCacheTests extends ESTestCase {
    IndexSearcher searcher;
    Directory dir;
    RandomIndexWriter w;
    DirectoryReader reader;

    @Before
    public void initLuceneStuff() throws IOException {
        dir = newDirectory();
        w = new RandomIndexWriter(random(), dir);
        reader = w.getReader();
        searcher = newSearcher(reader);
    }

    @After
    public void closeLuceneStuff() throws IOException {
        w.close();
        dir.close();
        reader.close();
    }

    public void testOptOutQueryCacheSafetyCheck() throws IOException {

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new TermQuery(new Term("foo", "bar")), BooleanClause.Occur.MUST);
        builder.add(new TermQuery(new Term("no", "baz")), BooleanClause.Occur.MUST_NOT);
        Weight weight = builder.build().createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);

        // whenever the allowed fields match the fields in the query and we do not deny access to any fields we allow caching.
        IndicesAccessControl.IndexAccessControl permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(new String[] { "foo", "no" }, null)),
            DocumentPermissions.allowAll()
        );
        assertTrue(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(new String[] { "foo", "no" }, new String[] {})),
            DocumentPermissions.allowAll()
        );
        assertTrue(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(new String[] { "*" }, new String[] {})),
            DocumentPermissions.allowAll()
        );
        assertTrue(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(new String[] { "*" }, null)),
            DocumentPermissions.allowAll()
        );
        assertTrue(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(new String[] { "*" }, new String[] { "oof" })),
            DocumentPermissions.allowAll()
        );
        assertTrue(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(new String[] { "f*", "n*" }, new String[] {})),
            DocumentPermissions.allowAll()
        );
        assertTrue(OptOutQueryCache.cachingIsSafe(weight, permissions));

        // check we don't cache if a field is not allowed
        permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(new String[] { "foo" }, null)),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(new String[] { "a*" }, new String[] { "aa" })),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(null, new String[] { "no" })),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(null, new String[] { "*" })),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(new String[] { "foo", "no" }, new String[] { "no" })),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(new String[] {}, new String[] {})),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));

        permissions = new IndicesAccessControl.IndexAccessControl(
            true,
            new FieldPermissions(fieldPermissionDef(new String[] {}, null)),
            DocumentPermissions.allowAll()
        );
        assertFalse(OptOutQueryCache.cachingIsSafe(weight, permissions));
    }

    public void testOptOutQueryCacheNoIndicesPermissions() {
        final Settings.Builder settings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0);
        final IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        final IndicesQueryCache indicesQueryCache = mock(IndicesQueryCache.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final OptOutQueryCache cache = new OptOutQueryCache(indexSettings, indicesQueryCache, threadContext);
        final Weight weight = mock(Weight.class);
        final QueryCachingPolicy policy = mock(QueryCachingPolicy.class);
        final Weight w = cache.doCache(weight, policy);
        assertSame(w, weight);
        verifyNoMoreInteractions(indicesQueryCache);
    }

    public void testOptOutQueryCacheIndexDoesNotHaveFieldLevelSecurity() {
        final Settings.Builder settings = Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0);
        final IndexMetadata indexMetadata = IndexMetadata.builder("index").settings(settings).build();
        final IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        final IndicesQueryCache indicesQueryCache = mock(IndicesQueryCache.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        final IndicesAccessControl.IndexAccessControl indexAccessControl = mock(IndicesAccessControl.IndexAccessControl.class);
        when(indexAccessControl.getFieldPermissions()).thenReturn(new FieldPermissions());
        final IndicesAccessControl indicesAccessControl = mock(IndicesAccessControl.class);
        when(indicesAccessControl.getIndexPermissions("index")).thenReturn(indexAccessControl);
        threadContext.putTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, indicesAccessControl);
        final OptOutQueryCache cache = new OptOutQueryCache(indexSettings, indicesQueryCache, threadContext);
        final Weight weight = mock(Weight.class);
        final QueryCachingPolicy policy = mock(QueryCachingPolicy.class);
        cache.doCache(weight, policy);
        verify(indicesQueryCache).doCache(same(weight), same(policy));
    }

    private static FieldPermissionsDefinition fieldPermissionDef(String[] granted, String[] denied) {
        return new FieldPermissionsDefinition(granted, denied);
    }
}
