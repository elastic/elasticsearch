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

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisMode;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.ReloadableCustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MapperServiceTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(InternalSettingsPlugin.class, ReloadableFilterPlugin.class);
    }

    public void testPreflightUpdateDoesNotChangeMapping() throws Throwable {
        final MapperService mapperService = createIndex("test1").mapperService();
        final CompressedXContent mapping = createMappingSpecifyingNumberOfFields(1);
        mapperService.merge("type", mapping, MergeReason.MAPPING_UPDATE_PREFLIGHT);
        assertThat("field was not created by preflight check", mapperService.fieldType("field0"), nullValue());
        mapperService.merge("type", mapping, MergeReason.MAPPING_UPDATE);
        assertThat("field was not created by mapping update", mapperService.fieldType("field0"), notNullValue());
    }

    /**
     * Test that we can have at least the number of fields in new mappings that are defined by "index.mapping.total_fields.limit".
     * Any additional field should trigger an IllegalArgumentException.
     */
    public void testTotalFieldsLimit() throws Throwable {
        int totalFieldsLimit = randomIntBetween(1, 10);
        Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), totalFieldsLimit)
            .build();
        createIndex("test1", settings).mapperService().merge("type", createMappingSpecifyingNumberOfFields(totalFieldsLimit),
                MergeReason.MAPPING_UPDATE);

        // adding one more field should trigger exception
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            createIndex("test2", settings).mapperService().merge("type",
                createMappingSpecifyingNumberOfFields(totalFieldsLimit + 1), updateOrPreflight());
        });
        assertTrue(e.getMessage(),
                e.getMessage().contains("Limit of total fields [" + totalFieldsLimit + "] has been exceeded"));
    }

    private CompressedXContent createMappingSpecifyingNumberOfFields(int numberOfFields) throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().startObject()
                .startObject("properties");
        for (int i = 0; i < numberOfFields; i++) {
            mappingBuilder.startObject("field" + i);
            mappingBuilder.field("type", randomFrom("long", "integer", "date", "keyword", "text"));
            mappingBuilder.endObject();
        }
        mappingBuilder.endObject().endObject();
        return new CompressedXContent(BytesReference.bytes(mappingBuilder));
    }

    public void testMappingDepthExceedsLimit() throws Throwable {
        IndexService indexService1 = createIndex("test1",
            Settings.builder().put(MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getKey(), 1).build());
        // no exception
        indexService1.mapperService().merge("type", createMappingSpecifyingNumberOfFields(1), MergeReason.MAPPING_UPDATE);

        CompressedXContent objectMapping = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("object1")
                        .field("type", "object")
                    .endObject()
                .endObject().endObject()));

        IndexService indexService2 = createIndex("test2");
        // no exception
        indexService2.mapperService().merge("type", objectMapping, MergeReason.MAPPING_UPDATE);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> indexService1.mapperService().merge("type", objectMapping, updateOrPreflight()));
        assertThat(e.getMessage(), containsString("Limit of mapping depth [1] has been exceeded"));
    }

    public void testUnmappedFieldType() {
        MapperService mapperService = createIndex("index").mapperService();
        assertThat(mapperService.unmappedFieldType("keyword"), instanceOf(KeywordFieldType.class));
        assertThat(mapperService.unmappedFieldType("long"), instanceOf(NumberFieldType.class));
    }

    public void testPartitionedConstraints() {
        // partitioned index must have routing
         IllegalArgumentException noRoutingException = expectThrows(IllegalArgumentException.class, () -> {
            client().admin().indices().prepareCreate("test-index")
                    .setMapping("{\"_doc\":{}}")
                    .setSettings(Settings.builder()
                        .put("index.number_of_shards", 4)
                        .put("index.routing_partition_size", 2))
                    .execute().actionGet();
        });
        assertTrue(noRoutingException.getMessage(), noRoutingException.getMessage().contains("must have routing"));

        // valid partitioned index
        assertTrue(client().admin().indices().prepareCreate("test-index")
            .setMapping("{\"_doc\":{\"_routing\":{\"required\":true}}}")
            .setSettings(Settings.builder()
                .put("index.number_of_shards", 4)
                .put("index.routing_partition_size", 2))
            .execute().actionGet().isAcknowledged());
    }

    public void testIndexSortWithNestedFields() throws IOException {
        Settings settings = Settings.builder()
            .put("index.sort.field", "foo")
            .build();
        IllegalArgumentException invalidNestedException = expectThrows(IllegalArgumentException.class,
           () -> createIndex("test", settings, "t", "nested_field", "type=nested", "foo", "type=keyword"));
        assertThat(invalidNestedException.getMessage(),
            containsString("cannot have nested fields when index sort is activated"));
        IndexService indexService =  createIndex("test", settings, "t", "foo", "type=keyword");
        CompressedXContent nestedFieldMapping = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
            .startObject("nested_field")
            .field("type", "nested")
            .endObject()
            .endObject().endObject()));
        invalidNestedException = expectThrows(IllegalArgumentException.class,
            () -> indexService.mapperService().merge("t", nestedFieldMapping,
                updateOrPreflight()));
        assertThat(invalidNestedException.getMessage(),
            containsString("cannot have nested fields when index sort is activated"));
    }

     public void testFieldAliasWithMismatchedNestedScope() throws Throwable {
        IndexService indexService = createIndex("test");
        MapperService mapperService = indexService.mapperService();

        CompressedXContent mapping = new CompressedXContent(BytesReference.bytes(
            XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("nested")
                        .field("type", "nested")
                        .startObject("properties")
                            .startObject("field")
                                .field("type", "text")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject()));

        mapperService.merge("type", mapping, MergeReason.MAPPING_UPDATE);

        CompressedXContent mappingUpdate = new CompressedXContent(BytesReference.bytes(
            XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("alias")
                        .field("type", "alias")
                        .field("path", "nested.field")
                    .endObject()
                .endObject()
            .endObject()));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> mapperService.merge("type", mappingUpdate, updateOrPreflight()));
        assertThat(e.getMessage(), containsString("Invalid [path] value [nested.field] for field alias [alias]"));
    }

    public void testTotalFieldsLimitWithFieldAlias() throws Throwable {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("properties")
                .startObject("alias")
                    .field("type", "alias")
                    .field("path", "field")
                .endObject()
                .startObject("field")
                    .field("type", "text")
                .endObject()
            .endObject()
        .endObject().endObject());

        int numberOfFieldsIncludingAlias = 2;
        createIndex("test1", Settings.builder()
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), numberOfFieldsIncludingAlias).build()).mapperService()
                        .merge("type", new CompressedXContent(mapping), MergeReason.MAPPING_UPDATE);

        // Set the total fields limit to the number of non-alias fields, to verify that adding
        // a field alias pushes the mapping over the limit.
        int numberOfNonAliasFields = 1;
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            createIndex("test2",
                    Settings.builder().put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), numberOfNonAliasFields).build())
                            .mapperService().merge("type", new CompressedXContent(mapping), updateOrPreflight());
        });
        assertEquals("Limit of total fields [" + numberOfNonAliasFields + "] has been exceeded", e.getMessage());
    }

    public void testFieldNameLengthLimit() throws Throwable {
        int maxFieldNameLength = randomIntBetween(15, 20);
        String testString = new String(new char[maxFieldNameLength + 1]).replace("\0", "a");
        Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), maxFieldNameLength)
            .build();
        MapperService mapperService = createIndex("test1", settings).mapperService();

        CompressedXContent mapping = new CompressedXContent(BytesReference.bytes(
            XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject("field")
                        .field("type", "text")
                    .endObject()
                .endObject()
            .endObject().endObject()));

        mapperService.merge("type", mapping, MergeReason.MAPPING_UPDATE);

        CompressedXContent mappingUpdate = new CompressedXContent(BytesReference.bytes(
            XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject(testString)
                        .field("type", "text")
                    .endObject()
                .endObject()
            .endObject()));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            mapperService.merge("type", mappingUpdate, updateOrPreflight());
        });

        assertEquals("Field name [" + testString + "] is longer than the limit of [" + maxFieldNameLength + "] characters",
            e.getMessage());
    }

    public void testObjectNameLengthLimit() throws Throwable {
        int maxFieldNameLength = randomIntBetween(15, 20);
        String testString = new String(new char[maxFieldNameLength + 1]).replace("\0", "a");
        Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), maxFieldNameLength)
            .build();
        MapperService mapperService = createIndex("test1", settings).mapperService();

        CompressedXContent mapping = new CompressedXContent(BytesReference.bytes(
            XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject(testString)
                        .field("type", "object")
                    .endObject()
                .endObject()
            .endObject().endObject()));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            mapperService.merge("type", mapping, updateOrPreflight());
        });

        assertEquals("Field name [" + testString + "] is longer than the limit of [" + maxFieldNameLength + "] characters",
            e.getMessage());
    }

    public void testAliasFieldNameLengthLimit() throws Throwable {
        int maxFieldNameLength = randomIntBetween(15, 20);
        String testString = new String(new char[maxFieldNameLength + 1]).replace("\0", "a");
        Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), maxFieldNameLength)
            .build();
        MapperService mapperService = createIndex("test1", settings).mapperService();

        CompressedXContent mapping = new CompressedXContent(BytesReference.bytes(
            XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject(testString)
                        .field("type", "alias")
                        .field("path", "field")
                    .endObject()
                    .startObject("field")
                        .field("type", "text")
                    .endObject()
                .endObject()
            .endObject().endObject()));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            mapperService.merge("type", mapping, updateOrPreflight());
        });

        assertEquals("Field name [" + testString + "] is longer than the limit of [" + maxFieldNameLength + "] characters",
            e.getMessage());
    }

    public void testMappingRecoverySkipFieldNameLengthLimit() throws Throwable {
        int maxFieldNameLength = randomIntBetween(15, 20);
        String testString = new String(new char[maxFieldNameLength + 1]).replace("\0", "a");
        Settings settings = Settings.builder().put(MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(), maxFieldNameLength)
            .build();
        MapperService mapperService = createIndex("test1", settings).mapperService();

        CompressedXContent mapping = new CompressedXContent(BytesReference.bytes(
            XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                    .startObject(testString)
                        .field("type", "text")
                    .endObject()
                .endObject()
            .endObject().endObject()));

        DocumentMapper documentMapper = mapperService.merge("type", mapping, MergeReason.MAPPING_RECOVERY);

        assertEquals(testString, documentMapper.mappers().getMapper(testString).simpleName());
    }

    public void testReloadSearchAnalyzers() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.analysis.analyzer.reloadableAnalyzer.type", "custom")
                .put("index.analysis.analyzer.reloadableAnalyzer.tokenizer", "standard")
                .putList("index.analysis.analyzer.reloadableAnalyzer.filter", "myReloadableFilter").build();

        MapperService mapperService = createIndex("test_index", settings).mapperService();
        CompressedXContent mapping = new CompressedXContent(BytesReference.bytes(
                XContentFactory.jsonBuilder().startObject().startObject("_doc")
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "text")
                            .field("analyzer", "simple")
                            .field("search_analyzer", "reloadableAnalyzer")
                            .field("search_quote_analyzer", "stop")
                        .endObject()
                        .startObject("otherField")
                            .field("type", "text")
                            .field("analyzer", "standard")
                            .field("search_analyzer", "simple")
                            .field("search_quote_analyzer", "reloadableAnalyzer")
                        .endObject()
                    .endObject()
                .endObject().endObject()));

        mapperService.merge("_doc", mapping, MergeReason.MAPPING_UPDATE);
        IndexAnalyzers current = mapperService.getIndexAnalyzers();

        ReloadableCustomAnalyzer originalReloadableAnalyzer = (ReloadableCustomAnalyzer) current.get("reloadableAnalyzer").analyzer();
        TokenFilterFactory[] originalTokenFilters = originalReloadableAnalyzer.getComponents().getTokenFilters();
        assertEquals(1, originalTokenFilters.length);
        assertEquals("myReloadableFilter", originalTokenFilters[0].name());

        // now reload, this should change the tokenfilterFactory inside the analyzer
        mapperService.reloadSearchAnalyzers(getInstanceFromNode(AnalysisRegistry.class));
        IndexAnalyzers updatedAnalyzers = mapperService.getIndexAnalyzers();
        assertSame(current, updatedAnalyzers);
        assertSame(current.getDefaultIndexAnalyzer(), updatedAnalyzers.getDefaultIndexAnalyzer());
        assertSame(current.getDefaultSearchAnalyzer(), updatedAnalyzers.getDefaultSearchAnalyzer());
        assertSame(current.getDefaultSearchQuoteAnalyzer(), updatedAnalyzers.getDefaultSearchQuoteAnalyzer());

        assertFalse(assertSameContainedFilters(originalTokenFilters, current.get("reloadableAnalyzer")));
        assertFalse(assertSameContainedFilters(originalTokenFilters,
            mapperService.fieldType("field").getTextSearchInfo().getSearchAnalyzer()));
        assertFalse(assertSameContainedFilters(originalTokenFilters,
            mapperService.fieldType("otherField").getTextSearchInfo().getSearchQuoteAnalyzer()));
    }

    private boolean assertSameContainedFilters(TokenFilterFactory[] originalTokenFilter, NamedAnalyzer updatedAnalyzer) {
        ReloadableCustomAnalyzer updatedReloadableAnalyzer = (ReloadableCustomAnalyzer) updatedAnalyzer.analyzer();
        TokenFilterFactory[] newTokenFilters = updatedReloadableAnalyzer.getComponents().getTokenFilters();
        assertEquals(originalTokenFilter.length, newTokenFilters.length);
        int i = 0;
        for (TokenFilterFactory tf : newTokenFilters ) {
            assertEquals(originalTokenFilter[i].name(), tf.name());
            if (originalTokenFilter[i] != tf) {
                return false;
            }
            i++;
        }
        return true;
    }

    private static MergeReason updateOrPreflight() {
        return randomFrom(MergeReason.MAPPING_UPDATE, MergeReason.MAPPING_UPDATE_PREFLIGHT);
    }

    public static final class ReloadableFilterPlugin extends Plugin implements AnalysisPlugin {

        @Override
        public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
            return Collections.singletonMap("myReloadableFilter", new AnalysisProvider<TokenFilterFactory>() {

                @Override
                public TokenFilterFactory get(IndexSettings indexSettings, Environment environment, String name, Settings settings)
                        throws IOException {
                    return new TokenFilterFactory() {

                        @Override
                        public String name() {
                            return "myReloadableFilter";
                        }

                        @Override
                        public TokenStream create(TokenStream tokenStream) {
                            return tokenStream;
                        }

                        @Override
                        public AnalysisMode getAnalysisMode() {
                            return AnalysisMode.SEARCH_TIME;
                        }
                    };
                }
            });
        }
    }

}
