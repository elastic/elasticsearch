/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

/**
 * {@code split_queries_on_whitespace} with the built-in {@code lowercase} normalizer, using analyzers built by the real
 * {@code AnalysisRegistry} rather than the hand-built ones in {@link KeywordFieldMapperTests}.
 */
public class KeywordFieldMapperLowercaseNormalizerTests extends MapperServiceTestCase {

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        try {
            return createTestAnalysis(indexSettings, nodeSettings).indexAnalyzers;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void testBuiltinLowercaseNormalizerSplitsQueriesOnWhitespace() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "keyword").field("normalizer", "lowercase").field("split_queries_on_whitespace", true))
        );
        assertTokenStreamContents(
            mapperService.fieldType("field").getTextSearchInfo().searchAnalyzer().tokenStream("field", "FOO bar"),
            new String[] { "foo", "bar" }
        );
        // locks down the documented 255-char split, same as a custom whitespace normalizer
        assertTokenStreamContents(
            mapperService.fieldType("field").getTextSearchInfo().searchAnalyzer().tokenStream("field", "a".repeat(300)),
            new String[] { "a".repeat(255), "a".repeat(45) }
        );
    }
}
