/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.percolator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PercolatorExecutionContextTests extends ESTestCase {
    public void testFailIfFieldMappingNotFound() {
        PercolatorExecutionContext context = new PercolatorExecutionContext(
            createSearchExecutionContext(IndexMetadata.INDEX_UUID_NA_VALUE, null),
            false
        );
        MappedFieldType fieldType = new TextFieldMapper.TextFieldType("text", randomBoolean());
        MappedFieldType result = context.getMappedFieldType("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        SearchExecutionContext finalContext = context;
        QueryShardException e = expectThrows(QueryShardException.class, () -> finalContext.getMappedFieldType("name", null));
        assertEquals("No field mapping can be found for the field with name [name]", e.getMessage());

        context = new PercolatorExecutionContext(context, false);
        result = context.getMappedFieldType("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        PercolatorExecutionContext finalContext1 = context;
        expectThrows(QueryShardException.class, () -> finalContext1.getMappedFieldType("name", null));

        context = new PercolatorExecutionContext(context, true);
        result = context.getMappedFieldType("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        result = context.getMappedFieldType("name", null);
        assertThat(result, notNullValue());
        assertThat(result, instanceOf(TextFieldMapper.TextFieldType.class));
        assertThat(result.name(), equalTo("name"));
    }

    public void testBuildAnonymousFieldType() {
        SearchExecutionContext context = createSearchExecutionContext("uuid", null);
        assertThat(context.buildAnonymousFieldType("keyword"), instanceOf(KeywordFieldMapper.KeywordFieldType.class));
        assertThat(context.buildAnonymousFieldType("long"), instanceOf(NumberFieldMapper.NumberFieldType.class));
    }

    public static SearchExecutionContext createSearchExecutionContext(String indexUuid, String clusterAlias) {
        return createSearchExecutionContext(indexUuid, clusterAlias, MappingLookup.EMPTY, Map.of());
    }

    private static SearchExecutionContext createSearchExecutionContext(
        String indexUuid,
        String clusterAlias,
        MappingLookup mappingLookup,
        Map<String, Object> runtimeMappings
    ) {
        IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder("index");
        indexMetadataBuilder.settings(indexSettings(Version.CURRENT, 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, indexUuid));
        IndexMetadata indexMetadata = indexMetadataBuilder.build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        MapperService mapperService = createMapperService(indexSettings, mappingLookup);
        final long nowInMillis = randomNonNegativeLong();
        return new SearchExecutionContext(
            0,
            0,
            indexSettings,
            null,
            (mappedFieldType, fdc) -> mappedFieldType.fielddataBuilder(fdc).build(null, null),
            mapperService,
            mappingLookup,
            null,
            null,
            XContentParserConfiguration.EMPTY,
            new NamedWriteableRegistry(Collections.emptyList()),
            null,
            null,
            () -> nowInMillis,
            clusterAlias,
            null,
            () -> true,
            null,
            runtimeMappings
        );
    }

    private static MapperService createMapperService(IndexSettings indexSettings, MappingLookup mappingLookup) {
        IndexAnalyzers indexAnalyzers = IndexAnalyzers.of(singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, null)));
        IndicesModule indicesModule = new IndicesModule(Collections.emptyList());
        MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
        Supplier<SearchExecutionContext> searchExecutionContextSupplier = () -> { throw new UnsupportedOperationException(); };
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexAnalyzers()).thenReturn(indexAnalyzers);
        when(mapperService.parserContext()).thenReturn(
            new MappingParserContext(
                null,
                type -> mapperRegistry.getMapperParser(type, indexSettings.getIndexVersionCreated()),
                mapperRegistry.getRuntimeFieldParsers()::get,
                indexSettings.getIndexVersionCreated(),
                () -> TransportVersion.CURRENT,
                searchExecutionContextSupplier,
                ScriptCompiler.NONE,
                indexAnalyzers,
                indexSettings,
                indexSettings.getMode().buildIdFieldMapper(() -> true)
            )
        );
        when(mapperService.isMultiField(anyString())).then(
            (Answer<Boolean>) invocation -> mappingLookup.isMultiField(invocation.getArgument(0))
        );
        return mapperService;
    }
}
