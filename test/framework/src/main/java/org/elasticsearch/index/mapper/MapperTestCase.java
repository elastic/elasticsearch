/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for testing {@link Mapper}s.
 */
public abstract class MapperTestCase extends ESTestCase {
    protected static final Settings SETTINGS = Settings.builder().put("index.version.created", Version.CURRENT).build();

    protected Collection<? extends Plugin> getPlugins() {
        return emptyList();
    }

    protected Settings getIndexSettings() {
        return SETTINGS;
    }

    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return new IndexAnalyzers(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Map.of(),
            Map.of()
        );
    }

    protected final String randomIndexOptions() {
        return randomFrom(new String[] { "docs", "freqs", "positions", "offsets" });
    }

    protected final DocumentMapper createDocumentMapper(XContentBuilder mappings) throws IOException {
        return createMapperService(mappings).documentMapper();
    }

    protected final MapperService createMapperService(XContentBuilder mappings) throws IOException {
        return createMapperService(getIndexSettings(), mappings);
    }

    /**
     * Create a {@link MapperService} like we would for an index.
     */
    protected final MapperService createMapperService(Settings settings, XContentBuilder mapping) throws IOException {
        IndexMetadata meta = IndexMetadata.builder("index")
            .settings(Settings.builder().put("index.version.created", Version.CURRENT))
            .numberOfReplicas(0)
            .numberOfShards(1)
            .build();
        IndexSettings indexSettings = new IndexSettings(meta, Settings.EMPTY);
        MapperRegistry mapperRegistry = new IndicesModule(
            getPlugins().stream().filter(p -> p instanceof MapperPlugin).map(p -> (MapperPlugin) p).collect(toList())
        ).getMapperRegistry();
        ScriptModule scriptModule = new ScriptModule(
            Settings.EMPTY,
            getPlugins().stream().filter(p -> p instanceof ScriptPlugin).map(p -> (ScriptPlugin) p).collect(toList())
        );
        ScriptService scriptService = new ScriptService(Settings.EMPTY, scriptModule.engines, scriptModule.contexts);
        SimilarityService similarityService = new SimilarityService(indexSettings, scriptService, Map.of());
        MapperService mapperService = new MapperService(
            indexSettings,
            createIndexAnalyzers(indexSettings),
            xContentRegistry(),
            similarityService,
            mapperRegistry,
            () -> { throw new UnsupportedOperationException(); },
            () -> true,
            scriptService
        );
        merge(mapperService, mapping);
        return mapperService;
    }

    protected final void withLuceneIndex(
        MapperService mapperService,
        CheckedConsumer<RandomIndexWriter, IOException> builder,
        CheckedConsumer<IndexReader, IOException> test
    ) throws IOException {
        try (
            Directory dir = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), dir, new IndexWriterConfig(mapperService.indexAnalyzer()))
        ) {
            builder.accept(iw);
            try (IndexReader reader = iw.getReader()) {
                test.accept(reader);
            }
        }
    }

    protected final SourceToParse source(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        build.accept(builder);
        builder.endObject();
        return new SourceToParse("test", "1", BytesReference.bytes(builder), XContentType.JSON);
    }

    /**
     * Merge a new mapping into the one in the provided {@link MapperService}.
     */
    protected final void merge(MapperService mapperService, XContentBuilder mapping) throws IOException {
        mapperService.merge(null, new CompressedXContent(BytesReference.bytes(mapping)), MergeReason.MAPPING_UPDATE);
    }

    protected final XContentBuilder mapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
        buildFields.accept(builder);
        return builder.endObject().endObject().endObject();
    }

    protected final XContentBuilder fieldMapping(CheckedConsumer<XContentBuilder, IOException> buildField) throws IOException {
        return mapping(b -> {
            b.startObject("field");
            buildField.accept(b);
            b.endObject();
        });
    }

    QueryShardContext createQueryShardContext(MapperService mapperService) {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.getMapperService()).thenReturn(mapperService);
        when(queryShardContext.fieldMapper(anyString())).thenAnswer(inv -> mapperService.fieldType(inv.getArguments()[0].toString()));
        when(queryShardContext.getIndexAnalyzers()).thenReturn(mapperService.getIndexAnalyzers());
        when(queryShardContext.getSearchQuoteAnalyzer(anyObject())).thenCallRealMethod();
        when(queryShardContext.getSearchAnalyzer(anyObject())).thenCallRealMethod();
        when(queryShardContext.getIndexSettings()).thenReturn(mapperService.getIndexSettings());
        when(queryShardContext.simpleMatchToIndexNames(anyObject())).thenAnswer(
            inv -> mapperService.simpleMatchToFullName(inv.getArguments()[0].toString())
        );
        return queryShardContext;
    }

    protected abstract void minimalMapping(XContentBuilder b) throws IOException;

    public final void testEmptyName() throws IOException {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
            b.startObject("");
            minimalMapping(b);
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
        assertParseMinimalWarnings();
    }

    public final void testMinimalSerializesToItself() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, ToXContent.EMPTY_PARAMS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, ToXContent.EMPTY_PARAMS);
        parsedFromOrig.endObject();
        assertEquals(Strings.toString(orig), Strings.toString(parsedFromOrig));
        assertParseMinimalWarnings();
    }

    protected void assertParseMinimalWarnings() {
        // Most mappers don't emit any warnings
    }

    /**
     * Override to disable testing {@code meta} in fields that don't support it.
     */
    protected boolean supportsMeta() {
        return true;
    }

    protected void metaMapping(XContentBuilder b) throws IOException {
        minimalMapping(b);
    }

    public final void testMeta() throws IOException {
        assumeTrue("Field doesn't support meta", supportsMeta());
        XContentBuilder mapping = fieldMapping(
            b -> {
                metaMapping(b);
                b.field("meta", Collections.singletonMap("foo", "bar"));
            }
        );
        MapperService mapperService = createMapperService(mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );

        mapping = fieldMapping(this::metaMapping);
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );

        mapping = fieldMapping(b -> {
            metaMapping(b);
            b.field("meta", Collections.singletonMap("baz", "quux"));
        });
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );
    }

    public static List<?> fetchSourceValue(FieldMapper mapper, Object sourceValue) {
        return fetchSourceValue(mapper, sourceValue, null);
    }

    public static List<?> fetchSourceValue(FieldMapper mapper, Object sourceValue, String format) {
        String field = mapper.name();

        MapperService mapperService = mock(MapperService.class);
        when(mapperService.sourcePath(field)).thenReturn(Set.of(field));

        ValueFetcher fetcher = mapper.valueFetcher(mapperService, format);
        SourceLookup lookup = new SourceLookup();
        lookup.setSource(Collections.singletonMap(field, sourceValue));
        return fetcher.fetchValues(lookup);
    }
}
