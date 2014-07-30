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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.lucene.all.AllField;
import org.elasticsearch.common.lucene.all.AllTermQuery;
import org.elasticsearch.common.lucene.all.AllTokenStream;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.mapper.internal.SizeFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleAllMapperTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testSimpleAllMappers() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        // One field is boosted so we should see AllTokenStream used:
        assertThat(field.tokenStream(docMapper.mappers().indexAnalyzer(), null), Matchers.instanceOf(AllTokenStream.class));
        AllEntries allEntries = field.getAllEntries();
        assertThat(allEntries.fields().size(), equalTo(3));
        assertThat(allEntries.fields().contains("address.last.location"), equalTo(true));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));
        FieldMapper mapper = docMapper.mappers().smartNameFieldMapper("_all");
        assertThat(field.fieldType().omitNorms(), equalTo(true));
        assertThat(mapper.queryStringTermQuery(new Term("_all", "foobar")), Matchers.instanceOf(AllTermQuery.class));
    }

    @Test
    public void testAllMappersNoBoost() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/noboost-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = field.getAllEntries();
        assertThat(allEntries.fields().size(), equalTo(3));
        assertThat(allEntries.fields().contains("address.last.location"), equalTo(true));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));
        FieldMapper mapper = docMapper.mappers().smartNameFieldMapper("_all");
        assertThat(field.fieldType().omitNorms(), equalTo(false));
        assertThat(mapper.queryStringTermQuery(new Term("_all", "foobar")), Matchers.instanceOf(TermQuery.class));
    }

    @Test
    public void testAllMappersTermQuery() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping_omit_positions_on_all.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = field.getAllEntries();
        assertThat(allEntries.fields().size(), equalTo(3));
        assertThat(allEntries.fields().contains("address.last.location"), equalTo(true));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));
        FieldMapper mapper = docMapper.mappers().smartNameFieldMapper("_all");
        assertThat(field.fieldType().omitNorms(), equalTo(false));
        assertThat(mapper.queryStringTermQuery(new Term("_all", "foobar")), Matchers.instanceOf(TermQuery.class));

    }

    // #6187: make sure we see AllTermQuery even when offsets are indexed in the _all field:
    @Test
    public void testAllMappersWithOffsetsTermQuery() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping_offsets_on_all.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        // _all field indexes positions, and mapping has boosts, so we should see AllTokenStream:
        assertThat(field.tokenStream(docMapper.mappers().indexAnalyzer(), null), Matchers.instanceOf(AllTokenStream.class));
        AllEntries allEntries = field.getAllEntries();
        assertThat(allEntries.fields().size(), equalTo(3));
        assertThat(allEntries.fields().contains("address.last.location"), equalTo(true));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));
        FieldMapper mapper = docMapper.mappers().smartNameFieldMapper("_all");
        assertThat(field.fieldType().omitNorms(), equalTo(false));
        assertThat(mapper.queryStringTermQuery(new Term("_all", "foobar")), Matchers.instanceOf(AllTermQuery.class));
    }

    // #6187: if _all doesn't index positions then we never use AllTokenStream, even if some fields have boost
    @Test
    public void testBoostWithOmitPositions() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping_boost_omit_positions_on_all.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        // _all field omits positions, so we should not get AllTokenStream even though fields are boosted
        assertThat(field.tokenStream(docMapper.mappers().indexAnalyzer(), null), Matchers.not(Matchers.instanceOf(AllTokenStream.class)));
    }

    // #6187: if no fields were boosted, we shouldn't use AllTokenStream
    @Test
    public void testNoBoost() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/noboost-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        // no fields have boost, so we should not see AllTokenStream:
        assertThat(field.tokenStream(docMapper.mappers().indexAnalyzer(), null), Matchers.not(Matchers.instanceOf(AllTokenStream.class)));
    }


    @Test
    public void testSimpleAllMappersWithReparse() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mapping.json");
        DocumentMapper docMapper = parser.parse(mapping);
        String builtMapping = docMapper.mappingSource().string();
        // reparse it
        DocumentMapper builtDocMapper = parser.parse(builtMapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = builtDocMapper.parse(new BytesArray(json)).rootDoc();

        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = field.getAllEntries();
        assertThat(allEntries.fields().size(), equalTo(3));
        assertThat(allEntries.fields().contains("address.last.location"), equalTo(true));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));
        assertThat(field.fieldType().omitNorms(), equalTo(true));
    }

    @Test
    public void testSimpleAllMappersWithStore() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/store-mapping.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = docMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = field.getAllEntries();
        assertThat(allEntries.fields().size(), equalTo(2));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));

        String text = field.stringValue();
        assertThat(text, equalTo(allEntries.buildText()));
        assertThat(field.fieldType().omitNorms(), equalTo(false));
    }

    @Test
    public void testSimpleAllMappersWithReparseWithStore() throws Exception {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/store-mapping.json");
        DocumentMapper docMapper = parser.parse(mapping);
        String builtMapping = docMapper.mappingSource().string();
        // reparse it
        DocumentMapper builtDocMapper = parser.parse(builtMapping);
        byte[] json = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/all/test1.json");
        Document doc = builtDocMapper.parse(new BytesArray(json)).rootDoc();

        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = field.getAllEntries();
        assertThat(allEntries.fields().size(), equalTo(2));
        assertThat(allEntries.fields().contains("name.last"), equalTo(true));
        assertThat(allEntries.fields().contains("simple1"), equalTo(true));

        String text = field.stringValue();
        assertThat(text, equalTo(allEntries.buildText()));
        assertThat(field.fieldType().omitNorms(), equalTo(false));
    }

    @Test
    public void testRandom() throws Exception {
        boolean omitNorms = false;
        boolean stored = false;
        boolean enabled = true;
        boolean autoBoost = false;
        boolean tv_stored = false;
        boolean tv_payloads = false;
        boolean tv_offsets = false;
        boolean tv_positions = false;
        String similarity = null;
        boolean fieldData = false;
        XContentBuilder mappingBuilder = jsonBuilder();
        mappingBuilder.startObject().startObject("test");
        List<Tuple<String, Boolean>> booleanOptionList = new ArrayList<>();
        boolean allDefault = true;
        if (frequently()) {
            allDefault = false;
            mappingBuilder.startObject("_all");
            if (randomBoolean()) {
                booleanOptionList.add(new Tuple<>("omit_norms", omitNorms = randomBoolean()));
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
                booleanOptionList.add(new Tuple<>("auto_boost", autoBoost = randomBoolean()));
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
            Collections.shuffle(booleanOptionList, getRandom());
            for (Tuple<String, Boolean> option : booleanOptionList) {
                mappingBuilder.field(option.v1(), option.v2().booleanValue());
            }
            tv_stored |= tv_positions || tv_payloads || tv_offsets;
            if (randomBoolean()) {
                mappingBuilder.field("similarity", similarity = randomBoolean() ? "BM25" : "TF/IDF");
            }
            if (randomBoolean()) {
                fieldData = true;
                mappingBuilder.startObject("fielddata");
                mappingBuilder.field("foo", "bar");
                mappingBuilder.endObject();
            }
            mappingBuilder.endObject();
        }

        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping = mappingBuilder.endObject().endObject().bytes().toUtf8();
        logger.info(mapping);
        DocumentMapper docMapper = parser.parse(mapping);
        String builtMapping = docMapper.mappingSource().string();
        // reparse it
        DocumentMapper builtDocMapper = parser.parse(builtMapping);

        byte[] json = jsonBuilder().startObject()
                .field("foo", "bar")
                .field("_id", 1)
                .field("foobar", "foobar")
                .endObject().bytes().toBytes();
        Document doc = builtDocMapper.parse(new BytesArray(json)).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        if (enabled) {
            assertThat(field.fieldType().omitNorms(), equalTo(omitNorms));
            assertThat(field.fieldType().stored(), equalTo(stored));
            assertThat(field.fieldType().storeTermVectorOffsets(), equalTo(tv_offsets));
            assertThat(field.fieldType().storeTermVectorPayloads(), equalTo(tv_payloads));
            assertThat(field.fieldType().storeTermVectorPositions(), equalTo(tv_positions));
            assertThat(field.fieldType().storeTermVectors(), equalTo(tv_stored));
            AllEntries allEntries = field.getAllEntries();
            assertThat(allEntries.fields().size(), equalTo(2));
            assertThat(allEntries.fields().contains("foobar"), equalTo(true));
            assertThat(allEntries.fields().contains("foo"), equalTo(true));
            if (!stored) {
                assertThat(field.stringValue(), nullValue());
            }
            String text = stored ? field.stringValue() : "bar foobar";
            assertThat(text.trim(), equalTo(allEntries.buildText().trim()));
        } else {
            assertThat(field, nullValue());
        }

        Term term = new Term("foo", "bar");
        Query query = builtDocMapper.allFieldMapper().queryStringTermQuery(term);
        if (autoBoost) {
            assertThat(query, equalTo((Query)new AllTermQuery(term)));
        } else {
            assertThat(query, equalTo((Query)new TermQuery(term)));
        }
        if (similarity == null || similarity.equals("TF/IDF")) {
            assertThat(builtDocMapper.allFieldMapper().similarity(), nullValue());
        }   else {
            assertThat(similarity, equalTo(builtDocMapper.allFieldMapper().similarity().name()));
        }
        assertThat(builtMapping.contains("fielddata"), is(fieldData));
        if (allDefault) {
            BytesStreamOutput bytesStreamOutput = new BytesStreamOutput(0);
            XContentBuilder b =  new XContentBuilder(XContentType.JSON.xContent(), bytesStreamOutput);
            XContentBuilder xContentBuilder = builtDocMapper.allFieldMapper().toXContent(b, ToXContent.EMPTY_PARAMS);
            xContentBuilder.flush();
            assertThat(bytesStreamOutput.size(), equalTo(0));
        }

    }

    @Test
    public void testMultiField_includeInAllSetToFalse() throws IOException {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/multifield-mapping_include_in_all_set_to_false.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
                .field("_id", "1")
                .field("foo")
                    .startObject()
                        .field("bar", "Elasticsearch rules!")
                    .endObject()
                .endObject();

        Document doc = docMapper.parse(builder.bytes()).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = field.getAllEntries();
        assertThat(allEntries.fields(), empty());
    }

    @Test
    public void testMultiField_defaults() throws IOException {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/multifield-mapping_default.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse(mapping);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject()
                .field("_id", "1")
                .field("foo")
                .startObject()
                .field("bar", "Elasticsearch rules!")
                .endObject()
                .endObject();

        Document doc = docMapper.parse(builder.bytes()).rootDoc();
        AllField field = (AllField) doc.getField("_all");
        AllEntries allEntries = field.getAllEntries();
        assertThat(allEntries.fields(), hasSize(1));
        assertThat(allEntries.fields(), hasItem("foo.bar"));
    }

    @Test(expected = MapperParsingException.class)
    public void testMisplacedTypeInRoot() throws IOException {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/misplaced_type_in_root.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("test", mapping);
    }

    // related to https://github.com/elasticsearch/elasticsearch/issues/5864
    @Test(expected = MapperParsingException.class)
    public void testMistypedTypeInRoot() throws IOException {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/mistyped_type_in_root.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("test", mapping);
    }

    // issue https://github.com/elasticsearch/elasticsearch/issues/5864
    @Test(expected = MapperParsingException.class)
    public void testMisplacedMappingAsRoot() throws IOException {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/misplaced_mapping_key_in_root.json");
        DocumentMapper docMapper = createIndex("test").mapperService().documentMapperParser().parse("test", mapping);
    }

    // issue https://github.com/elasticsearch/elasticsearch/issues/5864
    // test that RootObjectMapping still works
    @Test
    public void testRootObjectMapperPropertiesDoNotCauseException() throws IOException {
        DocumentMapperParser parser = createIndex("test").mapperService().documentMapperParser();
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/type_dynamic_template_mapping.json");
        parser.parse("test", mapping);
        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/type_dynamic_date_formats_mapping.json");
        parser.parse("test", mapping);
        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/type_date_detection_mapping.json");
        parser.parse("test", mapping);
        mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/all/type_numeric_detection_mapping.json");
        parser.parse("test", mapping);
    }

    // issue https://github.com/elasticsearch/elasticsearch/issues/5864
    @Test
    public void testRootMappersStillWorking() {
        String mapping = "{";
        Map<String, String> rootTypes = new HashMap<>();
        //just pick some example from DocumentMapperParser.rootTypeParsers
        rootTypes.put(SizeFieldMapper.NAME, "{\"enabled\" : true}");
        rootTypes.put(IndexFieldMapper.NAME, "{\"enabled\" : true}");
        rootTypes.put(SourceFieldMapper.NAME, "{\"enabled\" : true}");
        rootTypes.put(TypeFieldMapper.NAME, "{\"store\" : true}");
        rootTypes.put("include_in_all", "true");
        rootTypes.put("index_analyzer", "\"standard\"");
        rootTypes.put("search_analyzer", "\"standard\"");
        rootTypes.put("analyzer", "\"standard\"");
        rootTypes.put("dynamic_date_formats", "[\"yyyy-MM-dd\", \"dd-MM-yyyy\"]");
        rootTypes.put("numeric_detection", "true");
        rootTypes.put("dynamic_templates", "[]");
        for (String key : rootTypes.keySet()) {
            mapping += "\"" + key+ "\"" + ":" + rootTypes.get(key) + ",\n";
        }
        mapping += "\"properties\":{}}" ;
        createIndex("test").mapperService().documentMapperParser().parse("test", mapping);
    }
}
