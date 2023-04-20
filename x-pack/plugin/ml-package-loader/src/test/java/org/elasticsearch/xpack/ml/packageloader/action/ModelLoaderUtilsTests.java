/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.elasticsearch.test.ESTestCase;

import java.net.URI;
import java.net.URISyntaxException;

public class ModelLoaderUtilsTests extends ESTestCase {

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
}
