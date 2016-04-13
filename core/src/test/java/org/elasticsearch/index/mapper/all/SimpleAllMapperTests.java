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

package org.elasticsearch.index.mapper.all;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.all.AllTermQuery;
import org.elasticsearch.common.lucene.all.AllTokenStream;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.StreamsUtils.copyToBytesFromClasspath;
import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SimpleAllMapperTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testSimpleAllMappers() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("person", new CompressedXContent(mapping));
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse("test", "person", "1", new BytesArray(json)).rootDoc();
        IndexableField[] fields = doc.getFields("_all");
        assertThat(fields.length, equalTo(3));
        String[] expected = new String[] {"banon", "last location", "1"};
        for (int i = 0; i < fields.length; i++) {
            assertThat(fields[i].stringValue(), equalTo(expected[i]));
            assertThat(fields[i].fieldType().omitNorms(), equalTo(true));
            if (i == 0) {
                // The field "name.last" is boosted so we should see AllTokenStream used:
                assertThat(fields[i].tokenStream(docMapper.mappers().indexAnalyzer(), null),
                    Matchers.instanceOf(AllTokenStream.class));
            } else {
                assertThat(fields[i].tokenStream(docMapper.mappers().indexAnalyzer(), null),
                    Matchers.not(Matchers.instanceOf(AllTokenStream.class)));
            }
        }
        AllFieldMapper mapper = docMapper.allFieldMapper();
        assertThat(mapper.fieldType().queryStringTermQuery(new Term("_all", "foobar")),
            Matchers.instanceOf(AllTermQuery.class));
    }

    public void testAllMappersNoBoost() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/noboost-mapping.json");
        IndexService index = createIndex("test");
        DocumentMapper docMapper = index.mapperService().documentMapperParser().parse("person",
            new CompressedXContent(mapping));
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");

        Document doc = docMapper.parse("test", "person", "1", new BytesArray(json)).rootDoc();
        IndexableField[] fields = doc.getFields("_all");
        assertThat(fields.length, equalTo(3));
        String[] expected = new String[] {"banon", "last location", "1"};
        for (int i = 0; i < fields.length; i++) {
            assertThat(fields[i].stringValue(), equalTo(expected[i]));
            assertThat(fields[i].fieldType().omitNorms(), equalTo(false));
        }
        AllFieldMapper mapper = docMapper.allFieldMapper();
        assertThat(mapper.fieldType().queryStringTermQuery(new Term("_all", "foobar")),
            Matchers.instanceOf(AllTermQuery.class));
    }

    public void testAllMappersTermQuery() throws Exception {
        String mapping =
            copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping_omit_positions_on_all.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("person",
            new CompressedXContent(mapping));
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse("test", "person", "1", new BytesArray(json)).rootDoc();
        IndexableField[] fields = doc.getFields("_all");
        assertThat(fields.length, equalTo(3));
        String[] expected = new String[] {"banon", "last location", "1"};
        for (int i = 0; i < fields.length; i++) {
            assertThat(fields[i].stringValue(), equalTo(expected[i]));
            assertThat(fields[i].fieldType().omitNorms(), equalTo(false));
        }
        AllFieldMapper mapper = docMapper.allFieldMapper();
        assertThat(mapper.fieldType().queryStringTermQuery(new Term("_all", "foobar")),
            Matchers.instanceOf(AllTermQuery.class));
    }

    // #6187: make sure we see AllTermQuery even when offsets are indexed in the _all field:
    public void testAllMappersWithOffsetsTermQuery() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping_offsets_on_all.json");
        DocumentMapper docMapper =
            createIndex("test").mapperService().documentMapperParser().parse("person", new CompressedXContent(mapping));
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse("test", "person", "1", new BytesArray(json)).rootDoc();
        IndexableField[] fields = doc.getFields("_all");
        assertThat(fields.length, equalTo(3));
        String[] expected = new String[] {"banon", "last location", "1"};
        for (int i = 0; i < fields.length; i++) {
            assertThat(fields[i].stringValue(), equalTo(expected[i]));
            assertThat(fields[i].fieldType().omitNorms(), equalTo(false));
        }
        AllFieldMapper mapper = docMapper.allFieldMapper();
        assertThat(mapper.fieldType().queryStringTermQuery(new Term("_all", "foobar")),
            Matchers.instanceOf(AllTermQuery.class));
    }

    // #6187: if _all doesn't index positions then we never use AllTokenStream, even if some fields have boost
    public void testBoostWithOmitPositions() throws Exception {
        String mapping =
            copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping_boost_omit_positions_on_all.json");
        DocumentMapper docMapper =
            createIndex("test").mapperService().documentMapperParser().parse("person", new CompressedXContent(mapping));
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse("test", "person", "1", new BytesArray(json)).rootDoc();
        IndexableField[] fields = doc.getFields("_all");
        assertThat(fields.length, equalTo(3));
        for (IndexableField field : fields) {
            // _all field omits positions, so we should not get AllTokenStream even though fields are boosted
            assertThat(field.tokenStream(docMapper.mappers().indexAnalyzer(), null),
                Matchers.not(Matchers.instanceOf(AllTokenStream.class)));
        }
    }

    // #6187: if no fields were boosted, we shouldn't use AllTokenStream
    public void testNoBoost() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/noboost-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("person", new CompressedXContent(mapping));
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse("test", "person", "1", new BytesArray(json)).rootDoc();
        IndexableField[] fields = doc.getFields("_all");
        assertThat(fields.length, equalTo(3));
        for (IndexableField field : fields) {
            // no fields have boost, so we should not see AllTokenStream:
            assertThat(field.tokenStream(docMapper.mappers().indexAnalyzer(), null),
                Matchers.not(Matchers.instanceOf(AllTokenStream.class)));
        }
    }

    public void testSimpleAllMappersWithReparse() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping.json");
        DocumentMapper docMapper = parser.parse("person", new CompressedXContent(mapping));
        String builtMapping = docMapper.mappingSource().string();
        // reparse it
        DocumentMapper builtDocMapper = parser.parse("person", new CompressedXContent(builtMapping));
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = builtDocMapper.parse("test", "person", "1", new BytesArray(json)).rootDoc();
        IndexableField[] fields = doc.getFields("_all");
        assertThat(fields.length, equalTo(3));
        String[] expected = new String[] {"banon", "last location", "1"};
        for (int i = 0; i < fields.length; i++) {
            assertThat(fields[i].stringValue(), equalTo(expected[i]));
            assertThat(fields[i].fieldType().omitNorms(), equalTo(true));
            if (i == 0) {
                // The field "name.last" is boosted so we should see AllTokenStream used:
                assertThat(fields[i].tokenStream(docMapper.mappers().indexAnalyzer(), null),
                    Matchers.instanceOf(AllTokenStream.class));
            } else {
                assertThat(fields[i].tokenStream(docMapper.mappers().indexAnalyzer(), null),
                    Matchers.not(Matchers.instanceOf(AllTokenStream.class)));
            }
        }
    }

    public void testSimpleAllMappersWithStore() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/store-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("person", new CompressedXContent(mapping));
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse("test", "person", "1", new BytesArray(json)).rootDoc();
        IndexableField[] fields = doc.getFields("_all");
        assertThat(fields.length, equalTo(2));
        String[] expected = new String[] {"banon", "1"};
        for (int i = 0; i < fields.length; i++) {
            assertThat(fields[i].stringValue(), equalTo(expected[i]));
            assertThat(fields[i].fieldType().omitNorms(), equalTo(false));
        }
    }


    public void testSimpleAllMappersWithReparseWithStore() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/store-mapping.json");
        DocumentMapper docMapper = parser.parse("person", new CompressedXContent(mapping));
        String builtMapping = docMapper.mappingSource().string();
        // reparse it
        DocumentMapper builtDocMapper = parser.parse("person", new CompressedXContent(builtMapping));
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = builtDocMapper.parse("test", "person", "1", new BytesArray(json)).rootDoc();

        IndexableField[] fields = doc.getFields("_all");
        assertThat(fields.length, equalTo(2));
        String[] expected = new String[] {"banon", "1"};
        for (int i = 0; i < fields.length; i++) {
            assertThat(fields[i].stringValue(), equalTo(expected[i]));
            assertThat(fields[i].fieldType().omitNorms(), equalTo(false));
        }
    }

    public void testRandom() throws Exception {
        boolean norms = true;
        boolean stored = false;
        boolean enabled = true;
        boolean tv_stored = false;
        boolean tv_payloads = false;
        boolean tv_offsets = false;
        boolean tv_positions = false;
        String similarity = null;
        XContentBuilder mappingBuilder = jsonBuilder();
        mappingBuilder.startObject().startObject("test");
        List<Tuple<String, Boolean>> booleanOptionList = new ArrayList<>();
        boolean allDefault = true;
        if (frequently()) {
            allDefault = false;
            mappingBuilder.startObject("_all");
            if (randomBoolean()) {
                booleanOptionList.add(new Tuple<>("norms", norms = randomBoolean()));
            }
            if (randomBoolean()) {
                booleanOptionList.add(new Tuple<>("store", stored = randomBoolean()));
            }
            if (randomBoolean()) {
                booleanOptionList.add(new Tuple<>("store_term_vectors", tv_stored = randomBoolean()));
            }
            if (randomBoolean()) {
                booleanOptionList.add(new Tuple<>("enabled", enabled = randomBoolean()));
            }
            if (randomBoolean()) {
                booleanOptionList.add(new Tuple<>("store_term_vector_offsets", tv_offsets = randomBoolean()));
            }
            if (randomBoolean()) {
                booleanOptionList.add(new Tuple<>("store_term_vector_positions", tv_positions = randomBoolean()));
            }
            if (randomBoolean()) {
                booleanOptionList.add(new Tuple<>("store_term_vector_payloads", tv_payloads = randomBoolean()));
            }
            Collections.shuffle(booleanOptionList, random());
            for (Tuple<String, Boolean> option : booleanOptionList) {
                mappingBuilder.field(option.v1(), option.v2().booleanValue());
            }
            tv_stored |= tv_positions || tv_payloads || tv_offsets;
            if (randomBoolean()) {
                mappingBuilder.field("similarity", similarity = "BM25");
            }
            mappingBuilder.endObject();
        }

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping = mappingBuilder.endObject().endObject().bytes().toUtf8();
        logger.info("Mapping: {}", mapping);
        DocumentMapper docMapper = parser.parse("test", new CompressedXContent(mapping));
        String builtMapping = docMapper.mappingSource().string();
        // reparse it
        DocumentMapper builtDocMapper = parser.parse("test", new CompressedXContent(builtMapping));

        byte[] json = jsonBuilder().startObject()
                .field("foo", "bar")
                .field("foobar", "foobar")
                .endObject().bytes().toBytes();
        Document doc = builtDocMapper.parse("test", "test", "1", new BytesArray(json)).rootDoc();
        IndexableField[] fields = doc.getFields("_all");
        if (enabled) {
            assertThat(fields.length, equalTo(2));
            String[] expected = new String[] {"bar", "foobar"};
            for (int i = 0; i < fields.length; i++) {
                assertThat(fields[i].fieldType().omitNorms(), equalTo(!norms));
                assertThat(fields[i].fieldType().stored(), equalTo(stored));
                assertThat(fields[i].fieldType().storeTermVectorOffsets(), equalTo(tv_offsets));
                assertThat(fields[i].fieldType().storeTermVectorPayloads(), equalTo(tv_payloads));
                assertThat(fields[i].fieldType().storeTermVectorPositions(), equalTo(tv_positions));
                assertThat(fields[i].fieldType().storeTermVectors(), equalTo(tv_stored));
                assertThat(fields[i].stringValue(), equalTo(expected[i]));
            }
        } else {
            assertThat(fields.length, equalTo(0));
        }
        if (similarity == null) {
            assertThat(builtDocMapper.allFieldMapper().fieldType().similarity(), nullValue());
        }   else {
            assertThat(similarity, equalTo(builtDocMapper.allFieldMapper().fieldType().similarity().name()));
        }
        if (allDefault) {
            BytesStreamOutput bytesStreamOutput = new BytesStreamOutput(0);
            XContentBuilder b =  new XContentBuilder(XContentType.JSON.xContent(), bytesStreamOutput);
            XContentBuilder xContentBuilder = builtDocMapper.allFieldMapper().toXContent(b, ToXContent.EMPTY_PARAMS);
            xContentBuilder.flush();
            assertThat(bytesStreamOutput.size(), equalTo(0));
        }

    }

    public void testMultiField_includeInAllSetToFalse() throws IOException {
        String mapping =
            copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/" +
                "multifield-mapping_include_in_all_set_to_false.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("test", new CompressedXContent(mapping));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
                .field("foo")
                    .startObject()
                        .field("bar", "Elasticsearch rules!")
                    .endObject()
                .endObject();

        Document doc = docMapper.parse("test", "test", "1", builder.bytes()).rootDoc();
        IndexableField[] fields = doc.getFields("_all");
        assertThat(fields.length, equalTo(0));
    }

    public void testMultiField_defaults() throws IOException {
        String mapping =
            copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/multifield-mapping_default.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser()
            .parse("test", new CompressedXContent(mapping));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
                .field("foo")
                .startObject()
                .field("bar", "Elasticsearch rules!")
                .endObject()
                .endObject();

        Document doc = docMapper.parse("test", "test", "1", builder.bytes()).rootDoc();
        IndexableField[] fields = doc.getFields("_all");
        assertThat(fields.length, equalTo(1));
        assertThat(fields[0].stringValue(), equalTo("Elasticsearch rules!"));
    }

    public void testMisplacedTypeInRoot() throws IOException {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/misplaced_type_in_root.json");
        try {
            createIndex("test").mapperService().documentMapperParser().parse("test", new CompressedXContent(mapping));
            fail("Expected MapperParsingException");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), containsString("Root mapping definition has unsupported parameters"));
            assertThat(e.getMessage(), containsString("[type : text]"));
        }
    }

    // related to https://github.com/elastic/elasticsearch/issues/5864
    public void testMistypedTypeInRoot() throws IOException {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mistyped_type_in_root.json");
        try {
            createIndex("test").mapperService().documentMapperParser().parse("test", new CompressedXContent(mapping));
            fail("Expected MapperParsingException");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), containsString("Root mapping definition has unsupported parameters"));
            assertThat(e.getMessage(), containsString("type=text"));
        }
    }

    // issue https://github.com/elastic/elasticsearch/issues/5864
    public void testMisplacedMappingAsRoot() throws IOException {
        String mapping =
            copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/misplaced_mapping_key_in_root.json");
        try {
            createIndex("test").mapperService().documentMapperParser().parse("test", new CompressedXContent(mapping));
            fail("Expected MapperParsingException");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), containsString("Root mapping definition has unsupported parameters"));
            assertThat(e.getMessage(), containsString("type=text"));
        }
    }

    // issue https://github.com/elastic/elasticsearch/issues/5864
    // test that RootObjectMapping still works
    public void testRootObjectMapperPropertiesDoNotCauseException() throws IOException {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping =
            copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/type_dynamic_template_mapping.json");
        parser.parse("test", new CompressedXContent(mapping));
        mapping =
            copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/type_dynamic_date_formats_mapping.json");
        parser.parse("test", new CompressedXContent(mapping));
        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/type_date_detection_mapping.json");
        parser.parse("test", new CompressedXContent(mapping));
        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/type_numeric_detection_mapping.json");
        parser.parse("test", new CompressedXContent(mapping));
    }

    // issue https://github.com/elastic/elasticsearch/issues/5864
    public void testMetadataMappersStillWorking() throws MapperParsingException, IOException {
        String mapping = "{";
        Map<String, String> rootTypes = new HashMap<>();
        //just pick some example from DocumentMapperParser.rootTypeParsers
        rootTypes.put(TimestampFieldMapper.NAME, "{\"enabled\" : true}");
        rootTypes.put("include_in_all", "true");
        rootTypes.put("dynamic_date_formats", "[\"yyyy-MM-dd\", \"dd-MM-yyyy\"]");
        rootTypes.put("numeric_detection", "true");
        rootTypes.put("dynamic_templates", "[]");
        for (String key : rootTypes.keySet()) {
            mapping += "\"" + key+ "\"" + ":" + rootTypes.get(key) + ",\n";
        }
        mapping += "\"properties\":{}}" ;
        createIndex("test").mapperService().documentMapperParser().parse("test", new CompressedXContent(mapping));
    }

    public void testDocValuesNotAllowed() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type")
            .startObject("_all")
                .field("doc_values", true)
            .endObject().endObject().endObject().string();
        try {
            createIndex("test").mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));
            fail();
        } catch (MapperParsingException e) {
            assertThat(e.getDetailedMessage(),
                containsString("[_all] is always tokenized and cannot have doc values"));
        }

    }

    public void testAutoBoost() throws Exception {
        for (boolean boost : new boolean[] {false, true}) {
            String index = "test_" + boost;
            IndexService indexService =
                createIndex(index, client().admin().indices().prepareCreate(index)
                    .addMapping("type", "foo", "type=text" + (boost ? ",boost=2" : "")));
            client().prepareIndex(index, "type").setSource("foo", "bar").get();
            client().admin().indices().prepareRefresh(index).get();
            Query query = indexService.mapperService()
                .documentMapper("type").allFieldMapper().fieldType().termQuery("bar", null);
            try (Searcher searcher = indexService.getShardOrNull(0).acquireSearcher("tests")) {
                query = searcher.searcher().rewrite(query);
                final Class<?> expected = boost ? AllTermQuery.class : TermQuery.class;
                assertThat(query, Matchers.instanceOf(expected));
            }
        }
    }

    public void testIncludeInObjectNotAllowed() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("type").endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test").mapperService()
            .documentMapperParser().parse("type", new CompressedXContent(mapping));

        try {
            docMapper.parse("test", "type", "1", XContentFactory.jsonBuilder()
                .startObject().field("_all", "foo").endObject().bytes());
            fail("Expected failure to parse metadata field");
        } catch (MapperParsingException e) {
            assertTrue(e.getMessage(),
                e.getMessage().contains("Field [_all] is a metadata field and cannot be added inside a document"));
        }
    }
}
