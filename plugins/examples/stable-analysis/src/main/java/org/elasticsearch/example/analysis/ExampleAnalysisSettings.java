/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.analysis;

import org.elasticsearch.plugin.settings.AnalysisSettings;
import org.elasticsearch.plugin.settings.BooleanSetting;
import org.elasticsearch.plugin.settings.IntSetting;
import org.elasticsearch.plugin.settings.LongSetting;
import org.elasticsearch.plugin.settings.StringSetting;
import org.elasticsearch.plugin.settings.ListSetting;

import java.util.List;

/**
 * This is a settings interface that will be injected into a plugins' constructors
 * annotated with @Inject.
 * The settings interface has to be annotated with @AnalysisSettings so that Elasticsearch can generate
 * its dynamic implementation.
 * Methods on the interface should be noarg and have a returned type.
 * For types supported see the plugin-api/org.elasticsearch.plugin.api.settings classes
 * @see CustomAnalyzerFactory an example injection of this interface
 */
@AnalysisSettings
public interface ExampleAnalysisSettings {
    /*
     * This method presents the use of String typed setting.
     * see the ReplaceCharWithNumberCharFilterFactory
     */
    @StringSetting(path = "old_char", defaultValue = " ")
    String oldCharToReplaceInCharFilter();

    /*
     * This method presents the use of int typed setting.
     * see the ReplaceCharWithNumberCharFilterFactory
     */
    @IntSetting(path = "new_number", defaultValue = 0)
    int newNumberToReplaceInCharFilter();

    /*
     * This method presents the use of long typed setting.
     * see the ExampleTokenFilterFactory
     */
    @LongSetting(path = "token_filter_number", defaultValue = 0L)
    long digitToSkipInTokenFilter();

    /*
     * This method presents the use of boolean typed setting.
     * see the ExampleAnalyzerFactory
     */
    @BooleanSetting(path = "analyzerUseTokenListOfChars", defaultValue = true)
    boolean analyzerUseTokenListOfChars();
    /*
     * This method presents the use of boolean typed setting.
     * see the ExampleTokenizerFactory
     */
    @ListSetting(path = "tokenizer_list_of_chars")
    List<String> singleCharsToSkipInTokenizer();
}
