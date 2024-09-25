/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.index.mapper.MapperService.MergeReason.MAPPING_UPDATE;

public class MultiFieldsTests extends ESTestCase {

    public void testMultiFieldsBuilderHasSyntheticSourceCompatibleKeywordField() {
        var isStored = randomBoolean();
        var hasNormalizer = randomBoolean();

        var builder = new FieldMapper.MultiFields.Builder();
        assertFalse(builder.hasSyntheticSourceCompatibleKeywordField());

        var keywordFieldMapperBuilder = getKeywordFieldMapperBuilder(isStored, hasNormalizer);
        builder.add(keywordFieldMapperBuilder);

        var expected = hasNormalizer == false;
        assertEquals(expected, builder.hasSyntheticSourceCompatibleKeywordField());
    }

    public void testMultiFieldsBuilderHasSyntheticSourceCompatibleKeywordFieldDuringMerge() {
        var isStored = randomBoolean();
        var hasNormalizer = randomBoolean();

        var builder = new TextFieldMapper.Builder("text_field", createDefaultIndexAnalyzers(), false);
        assertFalse(builder.multiFieldsBuilder.hasSyntheticSourceCompatibleKeywordField());

        var keywordFieldMapperBuilder = getKeywordFieldMapperBuilder(isStored, hasNormalizer);

        var newField = new TextFieldMapper.Builder("text_field", createDefaultIndexAnalyzers(), false).addMultiField(
            keywordFieldMapperBuilder
        ).build(MapperBuilderContext.root(false, false));

        builder.merge(
            newField,
            new FieldMapper.Conflicts("TextFieldMapper"),
            MapperMergeContext.root(false, false, MAPPING_UPDATE, Long.MAX_VALUE)
        );

        var expected = hasNormalizer == false;
        assertEquals(expected, builder.multiFieldsBuilder.hasSyntheticSourceCompatibleKeywordField());
    }

    private KeywordFieldMapper.Builder getKeywordFieldMapperBuilder(boolean isStored, boolean hasNormalizer) {
        var keywordFieldMapperBuilder = new KeywordFieldMapper.Builder(
            "field",
            IndexAnalyzers.of(Map.of(), Map.of("normalizer", Lucene.STANDARD_ANALYZER), Map.of()),
            ScriptCompiler.NONE,
            Integer.MAX_VALUE,
            IndexVersion.current()
        );
        if (isStored) {
            keywordFieldMapperBuilder.stored(true);
            if (randomBoolean()) {
                keywordFieldMapperBuilder.docValues(false);
            }
        }
        if (hasNormalizer) {
            keywordFieldMapperBuilder.normalizer("normalizer");
        }
        return keywordFieldMapperBuilder;
    }
}
