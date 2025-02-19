/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;

public class ModelLoaderUtilsTests extends ESTestCase {

    public void testGetInputStreamFromModelRepositoryThrowsUnsupportedScheme() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> ModelLoaderUtils.getInputStreamFromModelRepository(new URI("unknown:/home/ml/package.ext"))
        );

        assertThat(e.getMessage(), is("unsupported scheme"));
    }

    public void testResolvePackageLocationTrailingSlash() throws URISyntaxException {
        assertEquals(new URI("file:/home/ml/package.ext"), ModelLoaderUtils.resolvePackageLocation("file:///home/ml", "package.ext"));
        assertEquals(new URI("file:/home/ml/package.ext"), ModelLoaderUtils.resolvePackageLocation("file:///home/ml/", "package.ext"));
        assertEquals(
            new URI("http://my-package.repo/package.ext"),
            ModelLoaderUtils.resolvePackageLocation("http://my-package.repo", "package.ext")
        );
        assertEquals(
            new URI("http://my-package.repo/package.ext"),
            ModelLoaderUtils.resolvePackageLocation("http://my-package.repo/", "package.ext")
        );
        assertEquals(
            new URI("http://my-package.repo/sub/package.ext"),
            ModelLoaderUtils.resolvePackageLocation("http://my-package.repo/sub", "package.ext")
        );
        assertEquals(
            new URI("http://my-package.repo/sub/package.ext"),
            ModelLoaderUtils.resolvePackageLocation("http://my-package.repo/sub/", "package.ext")
        );
    }

    public void testResolvePackageLocationBreakout() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> ModelLoaderUtils.resolvePackageLocation("file:///home/ml/", "../package.ext")
        );

        assertEquals("Illegal path in package location", e.getMessage());

        e = expectThrows(
            IllegalArgumentException.class,
            () -> ModelLoaderUtils.resolvePackageLocation("file:///home/ml/", "http://foo.ba")
        );
        assertEquals("Illegal schema change in package location", e.getMessage());
    }

    public void testResolvePackageLocationNoSchemeInRepository() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> ModelLoaderUtils.resolvePackageLocation("/home/ml/", "package.ext")
        );
        assertEquals("Repository must contain a scheme", e.getMessage());
    }

    public void testSha256AndSize() throws IOException {
        byte[] bytes = randomByteArrayOfLength(randomIntBetween(10, 1_000_000));
        String expectedDigest = MessageDigests.toHexString(MessageDigests.sha256().digest(bytes));
        assertEquals(64, expectedDigest.length());

        int chunkSize = randomIntBetween(100, 10_000);
        int totalParts = (bytes.length + chunkSize - 1) / chunkSize;

        ModelLoaderUtils.InputStreamChunker inputStreamChunker = new ModelLoaderUtils.InputStreamChunker(
            new ByteArrayInputStream(bytes),
            chunkSize
        );

        for (int part = 0; part < totalParts - 1; ++part) {
            assertEquals(chunkSize, inputStreamChunker.next().length());
        }

        assertEquals(bytes.length - (chunkSize * (totalParts - 1)), inputStreamChunker.next().length());
        assertEquals(bytes.length, inputStreamChunker.getTotalBytesRead());
        assertEquals(expectedDigest, inputStreamChunker.getSha256());
    }

    public void testParseVocabulary() throws IOException {
        String vocabParts = """
            {
              "vocabulary": ["foo", "bar", "baz"],
              "merges": ["mergefoo", "mergebar", "mergebaz"],
              "scores": [1.0, 2.0, 3.0]
            }
            """;

        var is = new ByteArrayInputStream(vocabParts.getBytes(StandardCharsets.UTF_8));
        var parsedVocab = ModelLoaderUtils.parseVocabParts(is);
        assertThat(parsedVocab.vocab(), contains("foo", "bar", "baz"));
        assertThat(parsedVocab.merges(), contains("mergefoo", "mergebar", "mergebaz"));
        assertThat(parsedVocab.scores(), contains(1.0, 2.0, 3.0));
    }

    public void testSplitIntoRanges() {
        long totalSize = randomLongBetween(10_000, 50_000_000);
        int numStreams = randomIntBetween(1, 10);
        int chunkSize = 1024;
        var ranges = ModelLoaderUtils.split(totalSize, numStreams, chunkSize);
        assertThat(ranges, hasSize(numStreams + 1));

        int expectedNumChunks = (int) ((totalSize + chunkSize - 1) / chunkSize);
        assertThat(ranges.stream().mapToInt(ModelLoaderUtils.RequestRange::numParts).sum(), is(expectedNumChunks));

        long startBytes = 0;
        int startPartIndex = 0;
        for (int i = 0; i < ranges.size() - 1; i++) {
            assertThat(ranges.get(i).rangeStart(), is(startBytes));
            long end = startBytes + ((long) ranges.get(i).numParts() * chunkSize) - 1;
            assertThat(ranges.get(i).rangeEnd(), is(end));
            long expectedNumBytesInRange = (long) chunkSize * ranges.get(i).numParts() - 1;
            assertThat(ranges.get(i).rangeEnd() - ranges.get(i).rangeStart(), is(expectedNumBytesInRange));
            assertThat(ranges.get(i).startPart(), is(startPartIndex));

            startBytes = end + 1;
            startPartIndex += ranges.get(i).numParts();
        }

        var finalRange = ranges.get(ranges.size() - 1);
        assertThat(finalRange.rangeStart(), is(startBytes));
        assertThat(finalRange.rangeEnd(), is(totalSize - 1));
        assertThat(finalRange.numParts(), is(1));
    }

    public void testSplitIntoRanges_numRangesSmallerThanNumStreams() {
        long totalSize = 2_142;
        int numStreams = 10;
        int chunkSize = 1_000;
        var ranges = ModelLoaderUtils.split(totalSize, numStreams, chunkSize);
        assertThat(ranges.toString(), ranges, hasSize(3));
        assertThat(ranges.get(0).rangeStart(), is(0L));
        assertThat(ranges.get(0).rangeEnd(), is(999L));
        assertThat(ranges.get(0).numParts(), is(1));
        assertThat(ranges.get(1).rangeStart(), is(1000L));
        assertThat(ranges.get(1).rangeEnd(), is(1999L));
        assertThat(ranges.get(1).numParts(), is(1));
        assertThat(ranges.get(2).rangeStart(), is(2000L));
        assertThat(ranges.get(2).rangeEnd(), is(2141L));
        assertThat(ranges.get(2).numParts(), is(1));
    }

    public void testRangeRequestBytesRange() {
        long start = randomLongBetween(0, 2 << 10);
        long end = randomLongBetween(start + 1, 2 << 11);
        assertEquals("bytes=" + start + "-" + end, new ModelLoaderUtils.RequestRange(start, end, 0, 1).bytesRange());
    }
}
