/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;

public class ElisionFilterFactoryTests extends ESTokenStreamTestCase {

    public void testElisionFilterWithNoArticles() throws IOException {
        Settings settings = Settings.builder()
            .put("index.analysis.filter.elision.type", "elision")
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin())
        );

        assertEquals("elision filter requires [articles] or [articles_path] setting", e.getMessage());
    }

}
