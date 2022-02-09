/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

public class ScoreFunctionBuilderTests extends ESTestCase {

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new RandomScoreFunctionBuilder().seed(null));
        expectThrows(IllegalArgumentException.class, () -> new ScriptScoreFunctionBuilder((Script) null));
        expectThrows(IllegalArgumentException.class, () -> new FieldValueFactorFunctionBuilder((String) null));
        expectThrows(IllegalArgumentException.class, () -> new FieldValueFactorFunctionBuilder("").modifier(null));
        expectThrows(IllegalArgumentException.class, () -> new GaussDecayFunctionBuilder(null, "", "", ""));
        expectThrows(IllegalArgumentException.class, () -> new GaussDecayFunctionBuilder("", "", null, ""));
        expectThrows(IllegalArgumentException.class, () -> new GaussDecayFunctionBuilder("", "", null, "", randomDouble()));
        expectThrows(IllegalArgumentException.class, () -> new LinearDecayFunctionBuilder(null, "", "", ""));
        expectThrows(IllegalArgumentException.class, () -> new LinearDecayFunctionBuilder("", "", null, ""));
        expectThrows(IllegalArgumentException.class, () -> new LinearDecayFunctionBuilder("", "", null, "", randomDouble()));
        expectThrows(IllegalArgumentException.class, () -> new ExponentialDecayFunctionBuilder(null, "", "", ""));
        expectThrows(IllegalArgumentException.class, () -> new ExponentialDecayFunctionBuilder("", "", null, ""));
        expectThrows(IllegalArgumentException.class, () -> new ExponentialDecayFunctionBuilder("", "", null, "", randomDouble()));
    }

    public void testRandomScoreFunctionWithSeedNoField() throws Exception {
        RandomScoreFunctionBuilder builder = new RandomScoreFunctionBuilder();
        builder.seed(42);
        SearchExecutionContext context = Mockito.mock(SearchExecutionContext.class);
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexSettings settings = new IndexSettings(IndexMetadata.builder("index").settings(indexSettings).build(), Settings.EMPTY);
        Mockito.when(context.index()).thenReturn(settings.getIndex());
        Mockito.when(context.getShardId()).thenReturn(0);
        Mockito.when(context.getIndexSettings()).thenReturn(settings);
        Mockito.when(context.getFieldType(IdFieldMapper.NAME)).thenReturn(new KeywordFieldMapper.KeywordFieldType(IdFieldMapper.NAME));
        Mockito.when(context.isFieldMapped(IdFieldMapper.NAME)).thenReturn(true);
        builder.toFunction(context);
        assertWarnings("As of version 7.0 Elasticsearch will require that a [field] parameter is provided when a [seed] is set");
    }

    public void testRandomScoreFunctionWithSeed() throws Exception {
        RandomScoreFunctionBuilder builder = new RandomScoreFunctionBuilder();
        builder.setField("foo");
        builder.seed(42);
        SearchExecutionContext context = Mockito.mock(SearchExecutionContext.class);
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IndexSettings settings = new IndexSettings(IndexMetadata.builder("index").settings(indexSettings).build(), Settings.EMPTY);
        Mockito.when(context.index()).thenReturn(settings.getIndex());
        Mockito.when(context.getShardId()).thenReturn(0);
        Mockito.when(context.getIndexSettings()).thenReturn(settings);
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("foo", NumberType.LONG);
        Mockito.when(context.getFieldType("foo")).thenReturn(ft);
        Mockito.when(context.isFieldMapped("foo")).thenReturn(true);
        builder.toFunction(context);
    }
}
