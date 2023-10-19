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

        ModelLoaderUtils.InputStreamChunker inputStreamChunker = new ModelLoaderUtils.InputStreamChunker(
            new ByteArrayInputStream(bytes),
            chunkSize
        );

        int totalParts = (bytes.length + chunkSize - 1) / chunkSize;

        for (int part = 0; part < totalParts - 1; ++part) {
            assertEquals(chunkSize, inputStreamChunker.next().length());
        }

        assertEquals(bytes.length - (chunkSize * (totalParts - 1)), inputStreamChunker.next().length());
        assertEquals(bytes.length, inputStreamChunker.getTotalBytesRead());
        assertEquals(expectedDigest, inputStreamChunker.getSha256());
    }
}
