/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_ANALYZER_NAME;
import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_SEARCH_ANALYZER_NAME;
import static org.elasticsearch.index.analysis.AnalysisRegistry.DEFAULT_SEARCH_QUOTED_ANALYZER_NAME;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TypeParsersTests extends ESTestCase {

    private static Map<String, NamedAnalyzer> defaultAnalyzers() {
        Map<String, NamedAnalyzer> analyzers = new HashMap<>();
        analyzers.put(DEFAULT_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, null));
        analyzers.put(DEFAULT_SEARCH_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, null));
        analyzers.put(DEFAULT_SEARCH_QUOTED_ANALYZER_NAME, new NamedAnalyzer("default", AnalyzerScope.INDEX, null));
        return analyzers;
    }

    public void testMultiFieldWithinMultiField() throws IOException {

        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .field("type", "keyword")
            .startObject("fields")
            .startObject("sub-field")
            .field("type", "keyword")
            .startObject("fields")
            .startObject("sub-sub-field")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        Mapper.TypeParser typeParser = KeywordFieldMapper.PARSER;

        // For indices created prior to 8.0, we should only emit a warning and not fail parsing.
        Map<String, Object> fieldNode = XContentHelper.convertToMap(BytesReference.bytes(mapping), true, mapping.contentType()).v2();

        MapperService mapperService = mock(MapperService.class);
        IndexAnalyzers indexAnalyzers = IndexAnalyzers.of(defaultAnalyzers());
        when(mapperService.getIndexAnalyzers()).thenReturn(indexAnalyzers);

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(metadata, Settings.EMPTY);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);

        IndexVersion olderVersion = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0).indexVersion;
        MappingParserContext olderContext = new MappingParserContext(
            null,
            type -> typeParser,
            type -> null,
            olderVersion,
            () -> TransportVersion.MINIMUM_COMPATIBLE,
            null,
            ScriptCompiler.NONE,
            mapperService.getIndexAnalyzers(),
            mapperService.getIndexSettings(),
            ProvidedIdFieldMapper.NO_FIELD_DATA
        );

        TextFieldMapper.PARSER.parse("some-field", fieldNode, olderContext);
        assertWarnings(
            "At least one multi-field, [sub-field], "
                + "was encountered that itself contains a multi-field. Defining multi-fields within a multi-field is deprecated "
                + "and is not supported for indices created in 8.0 and later. To migrate the mappings, all instances of [fields] "
                + "that occur within a [fields] block should be removed from the mappings, either by flattening the chained "
                + "[fields] blocks into a single level, or switching to [copy_to] if appropriate."
        );

        // For indices created in 8.0 or later, we should throw an error.
        Map<String, Object> fieldNodeCopy = XContentHelper.convertToMap(BytesReference.bytes(mapping), true, mapping.contentType()).v2();

        IndexVersion version = IndexVersionUtils.randomVersionBetween(random(), IndexVersion.V_8_0_0, IndexVersion.current());
        TransportVersion transportVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.V_8_0_0,
            TransportVersion.current()
        );
        MappingParserContext context = new MappingParserContext(
            null,
            type -> typeParser,
            type -> null,
            version,
            () -> transportVersion,
            null,
            ScriptCompiler.NONE,
            mapperService.getIndexAnalyzers(),
            mapperService.getIndexSettings(),
            ProvidedIdFieldMapper.NO_FIELD_DATA
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            TextFieldMapper.PARSER.parse("textField", fieldNodeCopy, context);
        });
        assertThat(
            e.getMessage(),
            equalTo(
                "Encountered a multi-field [sub-field] which itself contains a "
                    + "multi-field. Defining chained multi-fields is not supported."
            )
        );
    }

    public void testParseMeta() {
        {
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> TypeParsers.parseMeta("foo", 3));
            assertEquals("[meta] must be an object, got Integer[3] for field [foo]", e.getMessage());
        }

        {
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> TypeParsers.parseMeta("foo", Map.of("veryloooooooooooongkey", 3L))
            );
            assertEquals("[meta] keys can't be longer than 20 chars, but got [veryloooooooooooongkey] for field [foo]", e.getMessage());
        }

        {
            Map<String, Object> mapping = Map.of("foo1", 3L, "foo2", 4L, "foo3", 5L, "foo4", 6L, "foo5", 7L, "foo6", 8L);
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> TypeParsers.parseMeta("foo", mapping));
            assertEquals("[meta] can't have more than 5 entries, but got 6 on field [foo]", e.getMessage());
        }

        {
            Map<String, Object> mapping = Map.of("foo", Map.of("bar", "baz"));
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> TypeParsers.parseMeta("foo", mapping));
            assertEquals("[meta] values can only be strings, but got Map1[{bar=baz}] for field [foo]", e.getMessage());
        }

        {
            Map<String, Object> mapping = Map.of("bar", "baz", "foo", 3);
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> TypeParsers.parseMeta("foo", mapping));
            assertEquals("[meta] values can only be strings, but got Integer[3] for field [foo]", e.getMessage());
        }

        {
            Map<String, String> meta = new HashMap<>();
            meta.put("foo", null);
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> TypeParsers.parseMeta("foo", meta));
            assertEquals("[meta] values can't be null (field [foo])", e.getMessage());
        }

        {
            String longString = IntStream.range(0, 51).mapToObj(Integer::toString).collect(Collectors.joining());
            Map<String, Object> mapping = Map.of("foo", longString);
            MapperParsingException e = expectThrows(MapperParsingException.class, () -> TypeParsers.parseMeta("foo", mapping));
            assertThat(e.getMessage(), Matchers.startsWith("[meta] values can't be longer than 50 chars"));
        }
    }
}
