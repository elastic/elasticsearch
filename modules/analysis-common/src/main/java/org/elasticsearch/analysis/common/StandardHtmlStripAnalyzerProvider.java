/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.CharArraySet;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.Analysis;

public class StandardHtmlStripAnalyzerProvider extends AbstractIndexAnalyzerProvider<StandardHtmlStripAnalyzer> {

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(StandardHtmlStripAnalyzerProvider.class);

    private final StandardHtmlStripAnalyzer analyzer;

    /**
     * @deprecated in 6.5, can not create in 7.0, and we remove this in 8.0
     */
    @Deprecated
    StandardHtmlStripAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        final CharArraySet defaultStopwords = CharArraySet.EMPTY_SET;
        CharArraySet stopWords = Analysis.parseStopWords(env, settings, defaultStopwords);
        analyzer = new StandardHtmlStripAnalyzer(stopWords);
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_7_0_0)) {
            throw new IllegalArgumentException(
                "[standard_html_strip] analyzer is not supported for new indices, "
                    + "use a custom analyzer using [standard] tokenizer and [html_strip] char_filter, plus [lowercase] filter"
            );
        } else {
            DEPRECATION_LOGGER.critical(
                DeprecationCategory.ANALYSIS,
                "standard_html_strip_deprecation",
                "Deprecated analyzer [standard_html_strip] used, "
                    + "replace it with a custom analyzer using [standard] tokenizer and [html_strip] char_filter, plus [lowercase] filter"
            );
        }
    }

    @Override
    public StandardHtmlStripAnalyzer get() {
        return this.analyzer;
    }
}
