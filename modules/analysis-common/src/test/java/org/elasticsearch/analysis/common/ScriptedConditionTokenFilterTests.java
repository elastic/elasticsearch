package org.elasticsearch.analysis.common;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

public class ScriptedConditionTokenFilterTests extends ESTokenStreamTestCase {

    public void testSimpleCondition() throws Exception {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.filter.cond.type", "condition")
            .put("index.analysis.filter.cond.script", "return \"two\".equals(term)")
            .putList("index.analysis.filter.cond.filters", "uppercase")
            .put("index.analysis.analyzer.myAnalyzer.type", "custom")
            .put("index.analysis.analyzer.myAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.myAnalyzer.filter", "cond")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        ScriptService scriptService = new ScriptService()
    }

}
