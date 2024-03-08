/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchRequestInterceptorTests extends ESTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private MockLicenseState licenseState;
    private SearchRequestInterceptor interceptor;

    @Before
    public void init() {
        threadPool = new TestThreadPool("search request interceptor tests");
        licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        clusterService = mock(ClusterService.class);
        interceptor = new SearchRequestInterceptor(threadPool, licenseState, clusterService);
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    public void testForceExcludeDeletedDocs() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("myterms");
        termsAggregationBuilder.minDocCount(0);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);

        final DocumentPermissions documentPermissions = DocumentPermissions.filteredBy(Set.of(new BytesArray("""
            {"term":{"username":"foo"}}""")));
        final String index = randomAlphaOfLengthBetween(3, 8);
        final PlainActionFuture<Void> listener = new PlainActionFuture<>();
        assertFalse(termsAggregationBuilder.excludeDeletedDocs());
        interceptor.disableFeatures(
            searchRequest,
            Map.of(index, new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, documentPermissions)),
            listener
        );
        assertTrue(termsAggregationBuilder.excludeDeletedDocs()); // changed value
    }

    public void testNoForceExcludeDeletedDocs() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("myterms");
        termsAggregationBuilder.minDocCount(1);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);

        final DocumentPermissions documentPermissions = DocumentPermissions.filteredBy(Set.of(new BytesArray("""
            {"term":{"username":"foo"}}""")));
        final String index = randomAlphaOfLengthBetween(3, 8);
        final PlainActionFuture<Void> listener = new PlainActionFuture<>();
        assertFalse(termsAggregationBuilder.excludeDeletedDocs());
        interceptor.disableFeatures(
            searchRequest,
            Map.of(index, new IndicesAccessControl.IndexAccessControl(FieldPermissions.DEFAULT, documentPermissions)),
            listener
        );
        assertFalse(termsAggregationBuilder.excludeDeletedDocs()); // did not change value

        termsAggregationBuilder.minDocCount(0);
        interceptor.disableFeatures(
            searchRequest,
            Map.of(), // no DLS
            listener
        );
        assertFalse(termsAggregationBuilder.excludeDeletedDocs()); // did not change value
    }

}
