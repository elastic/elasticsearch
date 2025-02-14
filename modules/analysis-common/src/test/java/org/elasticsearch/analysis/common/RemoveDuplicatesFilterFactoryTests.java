/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;
import static org.hamcrest.Matchers.instanceOf;

public class RemoveDuplicatesFilterFactoryTests extends ESTokenStreamTestCase {

    public void testRemoveDuplicatesFilter() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .put("index.analysis.filter.removedups.type", "remove_duplicates")
            .build();
        ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, new CommonAnalysisPlugin());
        TokenFilterFactory tokenFilter = analysis.tokenFilter.get("removedups");
        assertThat(tokenFilter, instanceOf(RemoveDuplicatesTokenFilterFactory.class));

        CannedTokenStream cts = new CannedTokenStream(
            new Token("a", 1, 0, 1),
            new Token("b", 1, 2, 3),
            new Token("c", 0, 2, 3),
            new Token("b", 0, 2, 3),
            new Token("d", 1, 4, 5)
        );

        assertTokenStreamContents(tokenFilter.create(cts), new String[] { "a", "b", "c", "d" }, new int[] { 1, 1, 0, 1 });
    }

}
