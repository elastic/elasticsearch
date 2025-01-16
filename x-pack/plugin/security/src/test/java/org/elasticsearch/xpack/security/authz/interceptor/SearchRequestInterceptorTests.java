/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.core.Set;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.security.Security.DLS_FORCE_TERMS_AGGS_TO_EXCLUDE_DELETED_DOCS;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        interceptor = new SearchRequestInterceptor(threadPool, licenseState, clusterService);
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    private void configureMinMondeVersion(Version version) {
        final ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.getMinNodeVersion()).thenReturn(version);
    }

    public void testRequestCacheWillBeDisabledWhenMinNodeVersionIsBeforeShardSearchInterceptor() {
        configureMinMondeVersion(VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_11_1));
        final SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchRequest.indices()).thenReturn(randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)));
        when(searchRequest.source()).thenReturn(SearchSourceBuilder.searchSource());
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        interceptor.disableFeatures(searchRequest, org.elasticsearch.core.Map.of(), future);
        future.actionGet();
        verify(searchRequest).requestCache(false);
    }

    public void testRequestCacheWillBeDisabledWhenSearchRemoteIndices() {
        configureMinMondeVersion(VersionUtils.randomVersionBetween(random(), Version.V_7_11_2, Version.CURRENT));
        final SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchRequest.source()).thenReturn(SearchSourceBuilder.searchSource());
        final String[] localIndices = randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));
        final String[] remoteIndices = randomArray(
            0,
            3,
            String[]::new,
            () -> randomAlphaOfLengthBetween(0, 5) + ":" + randomAlphaOfLengthBetween(3, 8)
        );
        final ArrayList<String> allIndices = Arrays.stream(ArrayUtils.concat(localIndices, remoteIndices))
            .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(allIndices, random());
        when(searchRequest.indices()).thenReturn(allIndices.toArray(new String[0]));

        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        interceptor.disableFeatures(searchRequest, org.elasticsearch.core.Map.of(), future);
        future.actionGet();
        if (remoteIndices.length > 0) {
            verify(searchRequest).requestCache(false);
        } else {
            verify(searchRequest, never()).requestCache(anyBoolean());
        }
    }

    public void testHasRemoteIndices() {
        final SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchRequest.source()).thenReturn(SearchSourceBuilder.searchSource());
        final String[] localIndices = randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));
        final String[] remoteIndices = randomArray(
            0,
            3,
            String[]::new,
            () -> randomAlphaOfLengthBetween(0, 5) + ":" + randomAlphaOfLengthBetween(3, 8)
        );
        final ArrayList<String> allIndices = Arrays.stream(ArrayUtils.concat(localIndices, remoteIndices))
            .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(allIndices, random());
        when(searchRequest.indices()).thenReturn(allIndices.toArray(new String[0]));

        if (remoteIndices.length > 0) {
            assertThat(interceptor.hasRemoteIndices(searchRequest), is(true));
        } else {
            assertThat(interceptor.hasRemoteIndices(searchRequest), is(false));
        }
    }

    public void testForceExcludeDeletedDocs() {
        innerTestForceExcludeDeletedDocs(true);
    }

    public void testNoForceExcludeDeletedDocs() {
        configureMinMondeVersion(VersionUtils.randomVersionBetween(random(), Version.V_7_11_2, Version.CURRENT));
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("myterms");
        termsAggregationBuilder.minDocCount(1);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);

        final DocumentPermissions documentPermissions = DocumentPermissions.filteredBy(
            Set.of(new BytesArray("{\"term\":{\"username\":\"foo\"}}"))
        );
        final String index = randomAlphaOfLengthBetween(3, 8);
        final PlainActionFuture<Void> listener = new PlainActionFuture<>();
        assertFalse(termsAggregationBuilder.excludeDeletedDocs());
        interceptor.disableFeatures(
            searchRequest,
            Collections.singletonMap(
                index,
                new IndicesAccessControl.IndexAccessControl(false, FieldPermissions.DEFAULT, documentPermissions)
            ),
            listener
        );
        assertFalse(termsAggregationBuilder.excludeDeletedDocs()); // did not change value

        termsAggregationBuilder.minDocCount(0);
        interceptor.disableFeatures(
            searchRequest,
            Collections.emptyMap(), // no DLS
            listener
        );
        assertFalse(termsAggregationBuilder.excludeDeletedDocs()); // did not change value
    }

    public void testDisableFeaturesWithDLSConfig() {
        // default
        innerTestForceExcludeDeletedDocs(true);

        // explicit configuration - same as default
        when(clusterService.getSettings()).thenReturn(
            Settings.builder()
                .put(
                    DLS_FORCE_TERMS_AGGS_TO_EXCLUDE_DELETED_DOCS.getKey(),
                    DLS_FORCE_TERMS_AGGS_TO_EXCLUDE_DELETED_DOCS.getDefault(Settings.EMPTY)
                )
                .build()
        );
        interceptor = new SearchRequestInterceptor(threadPool, licenseState, clusterService);
        innerTestForceExcludeDeletedDocs(true);
        assertWarnings(
            "[xpack.security.dls.force_terms_aggs_to_exclude_deleted_docs.enabled] setting was deprecated in Elasticsearch and will be "
                + "removed in a future release! See the breaking changes documentation for the next major version."
        );

        // explicit configuration - opposite of default
        when(clusterService.getSettings()).thenReturn(
            Settings.builder()
                .put(
                    DLS_FORCE_TERMS_AGGS_TO_EXCLUDE_DELETED_DOCS.getKey(),
                    DLS_FORCE_TERMS_AGGS_TO_EXCLUDE_DELETED_DOCS.getDefault(Settings.EMPTY) == false
                )
                .build()
        );
        interceptor = new SearchRequestInterceptor(threadPool, licenseState, clusterService);
        innerTestForceExcludeDeletedDocs(false);
        assertWarnings(
            "[xpack.security.dls.force_terms_aggs_to_exclude_deleted_docs.enabled] setting was deprecated in Elasticsearch and will be "
                + "removed in a future release! See the breaking changes documentation for the next major version."
        );
    }

    private void innerTestForceExcludeDeletedDocs(boolean expectedToExcludeDeletedDocs) {
        configureMinMondeVersion(VersionUtils.randomVersionBetween(random(), Version.V_7_11_2, Version.CURRENT));
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("myterms");
        termsAggregationBuilder.minDocCount(0);
        searchSourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(searchSourceBuilder);

        final DocumentPermissions documentPermissions = DocumentPermissions.filteredBy(
            Set.of(new BytesArray("{\"term\":{\"username\":\"foo\"}}"))
        );
        final String index = randomAlphaOfLengthBetween(3, 8);
        final PlainActionFuture<Void> listener = new PlainActionFuture<>();
        assertFalse(termsAggregationBuilder.excludeDeletedDocs());
        interceptor.disableFeatures(
            searchRequest,
            Collections.singletonMap(
                index,
                new IndicesAccessControl.IndexAccessControl(false, FieldPermissions.DEFAULT, documentPermissions)
            ),
            listener
        );
        if (expectedToExcludeDeletedDocs) {
            assertTrue(termsAggregationBuilder.excludeDeletedDocs()); // changed value
        } else {
            assertFalse(termsAggregationBuilder.excludeDeletedDocs()); // did not change value
        }
    }
}
