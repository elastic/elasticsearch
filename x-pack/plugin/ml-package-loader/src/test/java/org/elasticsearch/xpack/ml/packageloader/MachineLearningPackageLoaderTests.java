/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class MachineLearningPackageLoaderTests extends ESTestCase {

    public void testValidateModelRepository() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> MachineLearningPackageLoader.validateModelRepository("file:///etc/passwd", PathUtils.get("/home/elk/elasticsearch"))
        );

        assertThat(
            e.getMessage(),
            is(
                oneOf(
                    "If xpack.ml.model_repository is a file location, it must be placed below the configuration: "
                        + "file:///home/elk/elasticsearch",
                    "If xpack.ml.model_repository is a file location, it must be placed below the configuration: "
                        + "file:///C:/home/elk/elasticsearch"
                )
            )
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> MachineLearningPackageLoader.validateModelRepository("file:///home/elk/", PathUtils.get("/home/elk/elasticsearch"))
        );

        assertThat(
            e.getMessage(),
            is(
                oneOf(
                    "If xpack.ml.model_repository is a file location, it must be placed below the configuration: "
                        + "file:///home/elk/elasticsearch",
                    "If xpack.ml.model_repository is a file location, it must be placed below the configuration: "
                        + "file:///C:/home/elk/elasticsearch"
                )
            )
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

        e = expectThrows(
            IllegalArgumentException.class,
            () -> MachineLearningPackageLoader.validateModelRepository(
                "http://user:pass@localhost",
                PathUtils.get("/home/elk/elasticsearch")
            )
        );

        assertEquals("xpack.ml.model_repository does not support authentication", e.getMessage());
    }

    public void testThreadPoolHasSingleThread() {
        var fixedThreadPool = MachineLearningPackageLoader.modelDownloadExecutor(Settings.EMPTY);
        List<Setting<?>> settings = fixedThreadPool.getRegisteredSettings();
        var sizeSettting = settings.stream().filter(s -> s.getKey().startsWith("xpack.ml.model_download_thread_pool")).findFirst();
        assertTrue(sizeSettting.isPresent());
        assertEquals(5, sizeSettting.get().get(Settings.EMPTY));
    }
}
