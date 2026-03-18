/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.common.lucene.search.function.RandomScoreFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.AbstractBuilderTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link RandomScoreFunctionBuilder} on an index with sequence numbers disabled.
 * The default _seq_no behavior is covered by {@link FunctionScoreQueryBuilderTests}.
 */
public class RandomScoreFunctionBuilderTests extends AbstractBuilderTestCase {

    @Override
    protected Settings createTestIndexSettings() {
        if (IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG == false) {
            return super.createTestIndexSettings();
        }
        return Settings.builder()
            .put("index.version.created", IndexVersion.current())
            .put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true)
            .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
            .build();
    }

    public void testRandomScoreWithoutFieldRequiresFieldWhenSeqNoDisabled() throws Exception {
        assumeTrue("Test requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);
        RandomScoreFunctionBuilder builder = new RandomScoreFunctionBuilder();
        builder.seed(42);
        SearchExecutionContext context = createSearchExecutionContext();
        assertTrue(context.getIndexSettings().sequenceNumbersDisabled());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.toFunction(context));
        assertThat(e.getMessage(), containsString("random_score requires a [field] parameter"));
        assertThat(e.getMessage(), containsString("index.disable_sequence_numbers"));
    }

    public void testRandomScoreWithExplicitFieldWhenSeqNoDisabled() throws Exception {
        assumeTrue("Test requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);
        RandomScoreFunctionBuilder builder = new RandomScoreFunctionBuilder();
        builder.seed(42);
        builder.setField(KEYWORD_FIELD_NAME);
        SearchExecutionContext context = createSearchExecutionContext();
        assertTrue(context.getIndexSettings().sequenceNumbersDisabled());
        ScoreFunction function = builder.toFunction(context);
        assertNotNull(function);
        assertThat(function, instanceOf(RandomScoreFunction.class));
    }

    public void testRandomScoreWithoutSeedFallsBackToDocIdWhenSeqNoDisabled() throws Exception {
        assumeTrue("Test requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);
        RandomScoreFunctionBuilder builder = new RandomScoreFunctionBuilder();
        SearchExecutionContext context = createSearchExecutionContext();
        assertTrue(context.getIndexSettings().sequenceNumbersDisabled());
        ScoreFunction function = builder.toFunction(context);
        assertNotNull(function);
        assertThat(function, instanceOf(RandomScoreFunction.class));
    }
}
