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

package org.elasticsearch.index.mapper.internal;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class FieldNamesFieldMapperTests extends ESSingleNodeTestCase {

    private static SortedSet<String> extract(String path) {
        SortedSet<String> set = new TreeSet<>();
        for (String fieldName : FieldNamesFieldMapper.extractFieldNames(path)) {
            set.add(fieldName);
        }
        return set;
    }

    private static <T> SortedSet<T> set(T... values) {
        return new TreeSet<>(Arrays.asList(values));
    }

    void assertFieldNames(SortedSet<String> expected, ParsedDocument doc) {
        String[] got = doc.rootDoc().getValues("_field_names");
        assertEquals(expected, set(got));
    }

    public void testExtractFieldNames() {
        assertEquals(set("abc"), extract("abc"));
        assertEquals(set("a", "a.b"), extract("a.b"));
        assertEquals(set("a", "a.b", "a.b.c"), extract("a.b.c"));
        // and now corner cases
        assertEquals(set("", ".a"), extract(".a"));
        assertEquals(set("a", "a."), extract("a."));
        assertEquals(set("", ".", ".."), extract(".."));
    }

    public void testFieldType() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").endObject()
            .endObject().endObject().string();

        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().hasDocValues());
        assertEquals(IndexOptions.DOCS, fieldNamesMapper.fieldType().indexOptions());
        assertFalse(fieldNamesMapper.fieldType().tokenized());
        assertFalse(fieldNamesMapper.fieldType().stored());
        assertTrue(fieldNamesMapper.fieldType().omitNorms());
    }

    public void testInjectIntoDocDuringParsing() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        DocumentMapper defaultMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        ParsedDocument doc = defaultMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .field("a", "100")
                    .startObject("b")
                        .field("c", 42)
                    .endObject()
                .endObject()
                .bytes());

        assertFieldNames(set("a", "b", "b.c", "_uid", "_type", "_version", "_source", "_all"), doc);
    }

    public void testExplicitEnabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", true).endObject()
            .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertTrue(fieldNamesMapper.fieldType().isEnabled());

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .endObject()
            .bytes());

        assertFieldNames(set("field", "_uid", "_type", "_version", "_source", "_all"), doc);
    }

    public void testDisabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", false).endObject()
            .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().isEnabled());

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .endObject()
            .bytes());

        assertNull(doc.rootDoc().get("_field_names"));
    }

    public void testPre13Disabled() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_2_4.id).build();
        DocumentMapper docMapper = createIndex("test", indexSettings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().isEnabled());
    }

    public void testDisablingBackcompat() throws Exception {
        // before 1.5, disabling happened by setting index:no
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("index", "no").endObject()
            .endObject().endObject().string();

        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        DocumentMapper docMapper = createIndex("test", indexSettings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertFalse(fieldNamesMapper.fieldType().isEnabled());

        ParsedDocument doc = docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
            .startObject()
            .field("field", "value")
            .endObject()
            .bytes());

        assertNull(doc.rootDoc().get("_field_names"));
    }

    public void testFieldTypeSettingsBackcompat() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("store", "yes").endObject()
            .endObject().endObject().string();

        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_4_2.id).build();
        DocumentMapper docMapper = createIndex("test", indexSettings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
        FieldNamesFieldMapper fieldNamesMapper = docMapper.metadataMapper(FieldNamesFieldMapper.class);
        assertTrue(fieldNamesMapper.fieldType().stored());
    }

    public void testMergingMappings() throws Exception {
        String enabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", true).endObject()
            .endObject().endObject().string();
        String disabledMapping = XContentFactory.jsonBuilder().startObject().startObject("type")
            .startObject("_field_names").field("enabled", false).endObject()
            .endObject().endObject().string();
        MapperService mapperService = createIndex("test").mapperService();

        DocumentMapper mapperEnabled = mapperService.merge("type", new CompressedXContent(enabledMapping), true, false);
        DocumentMapper mapperDisabled = mapperService.merge("type", new CompressedXContent(disabledMapping), false, false);
        assertFalse(mapperDisabled.metadataMapper(FieldNamesFieldMapper.class).fieldType().isEnabled());

        mapperEnabled = mapperService.merge("type", new CompressedXContent(enabledMapping), false, false);
        assertTrue(mapperEnabled.metadataMapper(FieldNamesFieldMapper.class).fieldType().isEnabled());
    }

    private static class DummyMetadataFieldMapper extends MetadataFieldMapper {

        public static class TypeParser implements MetadataFieldMapper.TypeParser {

            @Override
            public Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
                return new MetadataFieldMapper.Builder<Builder, DummyMetadataFieldMapper>("_dummy", FIELD_TYPE, FIELD_TYPE) {
                    @Override
                    public DummyMetadataFieldMapper build(BuilderContext context) {
                        return new DummyMetadataFieldMapper(context.indexSettings());
                    }
                };
            }

            @Override
            public MetadataFieldMapper getDefault(Settings indexSettings, MappedFieldType fieldType, String typeName) {
                return new DummyMetadataFieldMapper(indexSettings);
            }

        }

        private static class DummyFieldType extends MappedFieldType {

            public DummyFieldType() {
                super();
            }

            private DummyFieldType(MappedFieldType other) {
                super(other);
            }

            @Override
            public MappedFieldType clone() {
                return new DummyFieldType(this);
            }

            @Override
            public String typeName() {
                return "_dummy";
            }

        }

        private static final MappedFieldType FIELD_TYPE = new DummyFieldType();
        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setName("_dummy");
            FIELD_TYPE.freeze();
        }

        protected DummyMetadataFieldMapper(Settings indexSettings) {
            super("_dummy", FIELD_TYPE, FIELD_TYPE, indexSettings);
        }

        @Override
        public void preParse(ParseContext context) throws IOException {
        }

        @Override
        public void postParse(ParseContext context) throws IOException {
            context.doc().add(new Field("_dummy", "dummy", FIELD_TYPE));
        }

        @Override
        protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        }

        @Override
        protected String contentType() {
            return "_dummy";
        }

    }

    public void testSeesFieldsFromPlugins() throws IOException {
        IndexService indexService = createIndex("test");
        IndicesModule indicesModule = new IndicesModule();
        indicesModule.registerMetadataMapper("_dummy", new DummyMetadataFieldMapper.TypeParser());
        final MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
        MapperService mapperService = new MapperService(indexService.getIndexSettings(), indexService.analysisService(), indexService.similarityService(), mapperRegistry);
        DocumentMapperParser parser = new DocumentMapperParser(indexService.getIndexSettings(), mapperService,
                indexService.analysisService(), indexService.similarityService(), mapperRegistry);
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type").endObject().endObject().string();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping));
        ParsedDocument parsedDocument = mapper.parse("index", "type", "id", new BytesArray("{}"));
        IndexableField[] fields = parsedDocument.rootDoc().getFields(FieldNamesFieldMapper.NAME);
        boolean found = false;
        for (IndexableField f : fields) {
            if ("_dummy".equals(f.stringValue())) {
                found = true;
                break;
            }
        }
        assertTrue("Could not find the dummy field among " + Arrays.toString(fields), found);
    }
}
