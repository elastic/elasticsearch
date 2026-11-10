/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Paging behaviour of the S3 listing paths. ListObjectsV2 bounds a page's key count only from above, so a page may
 * carry no keys while more follow; continuation is decided by the continuation token, never by whether the page had
 * entries. The S3 client is mocked because the shared {@code S3HttpHandler} fixture always answers a listing in a
 * single page and cannot script multi-page shapes.
 */
public class S3StorageIteratorPaginationTests extends ESTestCase {

    private static final StoragePath PREFIX = StoragePath.of("s3://test-bucket/data");

    public void testMultiPageListingForwardsContinuationTokens() throws IOException {
        int pageCount = randomIntBetween(2, 6);
        List<ListObjectsV2Response> pages = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {
            expected.add("k" + i + ".csv");
            pages.add(
                i == pageCount - 1 ? lastPage(List.of("data/k" + i + ".csv")) : page(List.of("data/k" + i + ".csv"), "token-" + (i + 1))
            );
        }
        S3Client client = clientReturning(pages);

        assertEquals(expected, listNames(client));

        ArgumentCaptor<ListObjectsV2Request> requests = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(client, times(pageCount)).listObjectsV2(requests.capture());
        assertNull(requests.getAllValues().get(0).continuationToken());
        for (int i = 1; i < pageCount; i++) {
            assertEquals("token-" + i, requests.getAllValues().get(i).continuationToken());
        }
    }

    public void testEmptyTruncatedPageMidListingDoesNotEndListing() throws IOException {
        int emptyPages = randomIntBetween(1, 5);
        List<ListObjectsV2Response> pages = new ArrayList<>();
        pages.add(page(List.of("data/a.csv"), "token-1"));
        for (int i = 0; i < emptyPages; i++) {
            pages.add(page(List.of(), "token-" + (i + 2)));
        }
        pages.add(lastPage(List.of("data/b.csv")));
        S3Client client = clientReturning(pages);

        assertEquals(List.of("a.csv", "b.csv"), listNames(client));
        verify(client, times(pages.size())).listObjectsV2(any(ListObjectsV2Request.class));
    }

    public void testLeadingEmptyTruncatedPagesDoNotEndListing() throws IOException {
        int emptyPages = randomIntBetween(1, 5);
        List<ListObjectsV2Response> pages = new ArrayList<>();
        for (int i = 0; i < emptyPages; i++) {
            pages.add(page(List.of(), "token-" + (i + 1)));
        }
        pages.add(lastPage(List.of("data/a.csv")));
        S3Client client = clientReturning(pages);

        assertEquals(List.of("a.csv"), listNames(client));
        verify(client, times(pages.size())).listObjectsV2(any(ListObjectsV2Request.class));
    }

    public void testTruncatedPageWithoutContinuationTokenFails() throws IOException {
        assertTruncatedWithoutTokenFails(List.of(truncatedWithoutToken("data/a.csv")), List.of());
    }

    public void testPageWithoutIsTruncatedEndsListing() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            ListObjectsV2Response.builder().contents(objects(List.of("data/a.csv"))).build()
        );

        assertEquals(List.of("a.csv"), listNames(client));
        verify(client, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    public void testTruncatedPageWithoutContinuationTokenMidListingFails() throws IOException {
        assertTruncatedWithoutTokenFails(
            List.of(page(List.of("data/a.csv"), "token-1"), truncatedWithoutToken("data/b.csv")),
            List.of("a.csv")
        );
    }

    private void assertTruncatedWithoutTokenFails(List<ListObjectsV2Response> pages, List<String> expectedSeen) throws IOException {
        S3Client client = clientReturning(pages);
        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        List<String> seen = new ArrayList<>();
        try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
            UncheckedIOException thrown = expectThrows(UncheckedIOException.class, () -> {
                while (iterator.hasNext() && seen.size() < 10) {
                    seen.add(iterator.next().path().objectName());
                }
            });
            assertThat(thrown.getMessage(), containsString("without a continuation token"));
            assertThat(thrown.getMessage(), not(containsString("set [region]")));
        }
        assertEquals(expectedSeen, seen);
        verify(client, times(pages.size())).listObjectsV2(any(ListObjectsV2Request.class));
    }

    public void testFailureOnLaterPageResumesFromItsToken() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(page(List.of("data/a.csv"), "token-1"))
            .thenThrow(s3Failure(503))
            .thenReturn(lastPage(List.of("data/b.csv")));

        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        List<String> names = new ArrayList<>();
        try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
            assertTrue(iterator.hasNext());
            names.add(iterator.next().path().objectName());
            expectThrows(ExternalUnavailableException.class, iterator::hasNext);
            while (iterator.hasNext()) {
                names.add(iterator.next().path().objectName());
            }
        }

        assertEquals(List.of("a.csv", "b.csv"), names);
        ArgumentCaptor<ListObjectsV2Request> requests = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(client, times(3)).listObjectsV2(requests.capture());
        assertNull(requests.getAllValues().get(0).continuationToken());
        assertEquals("token-1", requests.getAllValues().get(1).continuationToken());
        assertEquals("token-1", requests.getAllValues().get(2).continuationToken());
    }

    public void testAccessDeniedListingNamesListBucketPermission() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenThrow(s3Failure(403));

        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
            UncheckedIOException thrown = expectThrows(UncheckedIOException.class, iterator::hasNext);
            assertThat(thrown.getMessage(), containsString("s3:ListBucket"));
        }
    }

    public void testNextPastEndThrows() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(lastPage(List.of("data/a.csv")));

        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
            iterator.next();
            expectThrows(NoSuchElementException.class, iterator::next);
        }
    }

    public void testListChildrenFollowsEmptyTruncatedPage() throws IOException {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            page(List.of("data/a.csv"), "token-1"),
            page(List.of(), "token-2"),
            ListObjectsV2Response.builder()
                .contents(objects(List.of("data/b.csv")))
                .commonPrefixes(CommonPrefix.builder().prefix("data/year=2026/").build())
                .isTruncated(false)
                .build()
        );

        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        StorageChildren children = provider.listChildren(PREFIX, 100);

        assertNotNull(children);
        assertEquals(List.of("a.csv", "b.csv"), children.files().stream().map(e -> e.path().objectName()).toList());
        assertEquals(List.of("year=2026"), children.directories().stream().map(StoragePath::objectName).toList());
        verify(client, times(3)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    public void testListChildrenTruncatedPageWithoutTokenFails() {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            ListObjectsV2Response.builder().contents(objects(List.of("data/a.csv"))).isTruncated(true).build()
        );

        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        IOException thrown = expectThrows(IOException.class, () -> provider.listChildren(PREFIX, 100));

        assertThat(thrown.getMessage(), containsString("without a continuation token"));
        verify(client, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
    }

    private static List<String> listNames(S3Client client) throws IOException {
        S3StorageProvider provider = S3StorageProvider.forTesting(client, null);
        List<String> names = new ArrayList<>();
        try (StorageIterator iterator = provider.listObjects(PREFIX, true)) {
            while (iterator.hasNext()) {
                StorageEntry entry = iterator.next();
                names.add(entry.path().objectName());
            }
        }
        return names;
    }

    private static S3Client clientReturning(List<ListObjectsV2Response> pages) {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(
            pages.get(0),
            pages.subList(1, pages.size()).toArray(new ListObjectsV2Response[0])
        );
        return client;
    }

    private static ListObjectsV2Response truncatedWithoutToken(String key) {
        return ListObjectsV2Response.builder().contents(objects(List.of(key))).isTruncated(true).build();
    }

    private static ListObjectsV2Response page(List<String> keys, String nextToken) {
        return ListObjectsV2Response.builder().contents(objects(keys)).isTruncated(true).nextContinuationToken(nextToken).build();
    }

    private static ListObjectsV2Response lastPage(List<String> keys) {
        return ListObjectsV2Response.builder().contents(objects(keys)).isTruncated(false).build();
    }

    private static List<S3Object> objects(List<String> keys) {
        return keys.stream().map(k -> S3Object.builder().key(k).size(1L).build()).toList();
    }

    private static S3Exception s3Failure(int status) {
        SdkHttpResponse response = SdkHttpResponse.builder().statusCode(status).build();
        return (S3Exception) S3Exception.builder()
            .statusCode(status)
            .message("S3 failure")
            .awsErrorDetails(AwsErrorDetails.builder().sdkHttpResponse(response).build())
            .build();
    }
}
