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

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin()));

        assertEquals("elision filter requires [articles] setting", e.getMessage());
    }

}
