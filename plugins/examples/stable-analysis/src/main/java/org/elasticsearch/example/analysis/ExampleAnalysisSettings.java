/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example.analysis;

import org.elasticsearch.plugin.api.settings.AnalysisSettings;
import org.elasticsearch.plugin.api.settings.BooleanSetting;
import org.elasticsearch.plugin.api.settings.IntSetting;
import org.elasticsearch.plugin.api.settings.LongSetting;
import org.elasticsearch.plugin.api.settings.StringSetting;
import org.elasticsearch.plugin.api.settings.ListSetting;

import java.util.List;

@AnalysisSettings
public interface ExampleAnalysisSettings {
    @StringSetting(path = "old_char", defaultValue = " ")
    String oldChar();

    @IntSetting(path = "new_number", defaultValue = 0)
    int newNumber();

    @LongSetting(path = "token_filter_number", defaultValue = 0L)
    long tokenFilterNumber();

    @BooleanSetting(path = "analyzerUseTokenListOfChars", defaultValue = true)
    boolean analyzerUseTokenListOfChars();

    @ListSetting(path = "tokenizer_list_of_chars")
    List<String> tokenizerListOfChars();
}
