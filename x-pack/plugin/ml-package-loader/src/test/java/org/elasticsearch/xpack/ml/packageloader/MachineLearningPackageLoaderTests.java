/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

public class MachineLearningPackageLoaderTests extends ESTestCase {

    public void testValidateModelRepository() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> MachineLearningPackageLoader.validateModelRepository("file:///etc/passwd", PathUtils.get("/home/elk/elasticsearch"))
        );

        assertEquals(
            "If xpack.ml.model_repository is a file location, it must be placed below the configuration: file:///home/elk/elasticsearch",
            e.getMessage()
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> MachineLearningPackageLoader.validateModelRepository("file:///home/elk/", PathUtils.get("/home/elk/elasticsearch"))
        );

        assertEquals(
            "If xpack.ml.model_repository is a file location, it must be placed below the configuration: file:///home/elk/elasticsearch",
            e.getMessage()
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> MachineLearningPackageLoader.validateModelRepository("elk/", PathUtils.get("/home/elk/elasticsearch"))
        );

        assertEquals(
            "xpack.ml.model_repository must contain a scheme, supported schemes are \"http\", \"https\", \"file\"",
            e.getMessage()
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> MachineLearningPackageLoader.validateModelRepository("mqtt://elky/", PathUtils.get("/home/elk/elasticsearch"))
        );

        assertEquals(
            "xpack.ml.model_repository must be configured with one of the following schemes: \"http\", \"https\", \"file\"",
            e.getMessage()
        );
    }
}
