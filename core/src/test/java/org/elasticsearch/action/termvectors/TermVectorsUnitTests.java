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

package org.elasticsearch.action.termvectors;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.termvectors.TermVectorsRequest.Flag;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.TypeParsers;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.rest.action.termvectors.RestTermVectorsAction;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TermVectorsUnitTests extends ElasticsearchTestCase {

    @Test
    public void streamResponse() throws Exception {

        TermVectorsResponse outResponse = new TermVectorsResponse("a", "b", "c");
        outResponse.setExists(true);
        writeStandardTermVector(outResponse);

        // write
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        outResponse.writeTo(out);

        // read
        ByteArrayInputStream esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput esBuffer = new InputStreamStreamInput(esInBuffer);
        TermVectorsResponse inResponse = new TermVectorsResponse("a", "b", "c");
        inResponse.readFrom(esBuffer);

        // see if correct
        checkIfStandardTermVector(inResponse);

        outResponse = new TermVectorsResponse("a", "b", "c");
        writeEmptyTermVector(outResponse);
        // write
        outBuffer = new ByteArrayOutputStream();
        out = new OutputStreamStreamOutput(outBuffer);
        outResponse.writeTo(out);

        // read
        esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        esBuffer = new InputStreamStreamInput(esInBuffer);
        inResponse = new TermVectorsResponse("a", "b", "c");
        inResponse.readFrom(esBuffer);
        assertTrue(inResponse.isExists());

    }

    private void writeEmptyTermVector(TermVectorsResponse outResponse) throws IOException {

        Directory dir = newDirectory();
        IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());
        conf.setOpenMode(OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(dir, conf);
        FieldType type = new FieldType(TextField.TYPE_STORED);
        type.setStoreTermVectorOffsets(true);
        type.setStoreTermVectorPayloads(false);
        type.setStoreTermVectorPositions(true);
        type.setStoreTermVectors(true);
        type.freeze();
        Document d = new Document();
        d.add(new Field("id", "abc", StringField.TYPE_STORED));

        writer.updateDocument(new Term("id", "abc"), d);
        writer.commit();
        writer.close();
        DirectoryReader dr = DirectoryReader.open(dir);
        IndexSearcher s = new IndexSearcher(dr);
        TopDocs search = s.search(new TermQuery(new Term("id", "abc")), 1);
        ScoreDoc[] scoreDocs = search.scoreDocs;
        int doc = scoreDocs[0].doc;
        Fields fields = dr.getTermVectors(doc);
        EnumSet<Flag> flags = EnumSet.of(Flag.Positions, Flag.Offsets);
        outResponse.setFields(fields, null, flags, fields);
        outResponse.setExists(true);
        dr.close();
        dir.close();

    }

    private void writeStandardTermVector(TermVectorsResponse outResponse) throws IOException {

        Directory dir = newDirectory();
        IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());

        conf.setOpenMode(OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(dir, conf);
        FieldType type = new FieldType(TextField.TYPE_STORED);
        type.setStoreTermVectorOffsets(true);
        type.setStoreTermVectorPayloads(false);
        type.setStoreTermVectorPositions(true);
        type.setStoreTermVectors(true);
        type.freeze();
        Document d = new Document();
        d.add(new Field("id", "abc", StringField.TYPE_STORED));
        d.add(new Field("title", "the1 quick brown fox jumps over  the1 lazy dog", type));
        d.add(new Field("desc", "the1 quick brown fox jumps over  the1 lazy dog", type));

        writer.updateDocument(new Term("id", "abc"), d);
        writer.commit();
        writer.close();
        DirectoryReader dr = DirectoryReader.open(dir);
        IndexSearcher s = new IndexSearcher(dr);
        TopDocs search = s.search(new TermQuery(new Term("id", "abc")), 1);
        ScoreDoc[] scoreDocs = search.scoreDocs;
        int doc = scoreDocs[0].doc;
        Fields termVectors = dr.getTermVectors(doc);
        EnumSet<Flag> flags = EnumSet.of(Flag.Positions, Flag.Offsets);
        outResponse.setFields(termVectors, null, flags, termVectors);
        dr.close();
        dir.close();

    }

    private void checkIfStandardTermVector(TermVectorsResponse inResponse) throws IOException {

        Fields fields = inResponse.getFields();
        assertThat(fields.terms("title"), Matchers.notNullValue());
        assertThat(fields.terms("desc"), Matchers.notNullValue());
        assertThat(fields.size(), equalTo(2));
    }

    @Test
    public void testRestRequestParsing() throws Exception {
        BytesReference inputBytes = new BytesArray(
                " {\"fields\" : [\"a\",  \"b\",\"c\"], \"offsets\":false, \"positions\":false, \"payloads\":true}");

        TermVectorsRequest tvr = new TermVectorsRequest(null, null, null);
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(inputBytes);
        TermVectorsRequest.parseRequest(tvr, parser);

        Set<String> fields = tvr.selectedFields();
        assertThat(fields.contains("a"), equalTo(true));
        assertThat(fields.contains("b"), equalTo(true));
        assertThat(fields.contains("c"), equalTo(true));
        assertThat(tvr.offsets(), equalTo(false));
        assertThat(tvr.positions(), equalTo(false));
        assertThat(tvr.payloads(), equalTo(true));
        String additionalFields = "b,c  ,d, e  ";
        RestTermVectorsAction.addFieldStringsFromParameter(tvr, additionalFields);
        assertThat(tvr.selectedFields().size(), equalTo(5));
        assertThat(fields.contains("d"), equalTo(true));
        assertThat(fields.contains("e"), equalTo(true));

        additionalFields = "";
        RestTermVectorsAction.addFieldStringsFromParameter(tvr, additionalFields);

        inputBytes = new BytesArray(" {\"offsets\":false, \"positions\":false, \"payloads\":true}");
        tvr = new TermVectorsRequest(null, null, null);
        parser = XContentFactory.xContent(XContentType.JSON).createParser(inputBytes);
        TermVectorsRequest.parseRequest(tvr, parser);
        additionalFields = "";
        RestTermVectorsAction.addFieldStringsFromParameter(tvr, additionalFields);
        assertThat(tvr.selectedFields(), equalTo(null));
        additionalFields = "b,c  ,d, e  ";
        RestTermVectorsAction.addFieldStringsFromParameter(tvr, additionalFields);
        assertThat(tvr.selectedFields().size(), equalTo(4));

    }

    @Test
    public void testRequestParsingThrowsException() throws Exception {
        BytesReference inputBytes = new BytesArray(
                " {\"fields\" : \"a,  b,c   \", \"offsets\":false, \"positions\":false, \"payloads\":true, \"meaningless_term\":2}");
        TermVectorsRequest tvr = new TermVectorsRequest(null, null, null);
        boolean threwException = false;
        try {
            XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(inputBytes);
            TermVectorsRequest.parseRequest(tvr, parser);
        } catch (Exception e) {
            threwException = true;
        }
        assertThat(threwException, equalTo(true));

    }

    @Test
    public void streamRequest() throws IOException {

        for (int i = 0; i < 10; i++) {
            TermVectorsRequest request = new TermVectorsRequest("index", "type", "id");
            request.offsets(random().nextBoolean());
            request.fieldStatistics(random().nextBoolean());
            request.payloads(random().nextBoolean());
            request.positions(random().nextBoolean());
            request.termStatistics(random().nextBoolean());
            String parent = random().nextBoolean() ? "someParent" : null;
            request.parent(parent);
            String pref = random().nextBoolean() ? "somePreference" : null;
            request.preference(pref);

            // write
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
            request.writeTo(out);

            // read
            ByteArrayInputStream esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
            InputStreamStreamInput esBuffer = new InputStreamStreamInput(esInBuffer);
            TermVectorsRequest req2 = new TermVectorsRequest(null, null, null);
            req2.readFrom(esBuffer);

            assertThat(request.offsets(), equalTo(req2.offsets()));
            assertThat(request.fieldStatistics(), equalTo(req2.fieldStatistics()));
            assertThat(request.payloads(), equalTo(req2.payloads()));
            assertThat(request.positions(), equalTo(req2.positions()));
            assertThat(request.termStatistics(), equalTo(req2.termStatistics()));
            assertThat(request.preference(), equalTo(pref));
            assertThat(request.routing(), equalTo(parent));

        }
    }
    
    @Test
    public void testFieldTypeToTermVectorString() throws Exception {
        FieldType ft = new FieldType();
        ft.setStoreTermVectorOffsets(false);
        ft.setStoreTermVectorPayloads(true);
        ft.setStoreTermVectors(true);
        ft.setStoreTermVectorPositions(true);
        String ftOpts = AbstractFieldMapper.termVectorOptionsToString(ft);
        assertThat("with_positions_payloads", equalTo(ftOpts));
        AllFieldMapper.Builder builder = new AllFieldMapper.Builder();
        boolean exceptiontrown = false;
        try {
            TypeParsers.parseTermVector("", ftOpts, builder);
        } catch (MapperParsingException e) {
            exceptiontrown = true;
        }
        assertThat("TypeParsers.parseTermVector should accept string with_positions_payloads but does not.", exceptiontrown, equalTo(false));
    }

    @Test
    public void testTermVectorStringGenerationWithoutPositions() throws Exception {
        FieldType ft = new FieldType();
        ft.setStoreTermVectorOffsets(true);
        ft.setStoreTermVectorPayloads(true);
        ft.setStoreTermVectors(true);
        ft.setStoreTermVectorPositions(false);
        String ftOpts = AbstractFieldMapper.termVectorOptionsToString(ft);
        assertThat(ftOpts, equalTo("with_offsets"));
    }

    @Test
    public void testMultiParser() throws Exception {
        byte[] data = Streams.copyToBytesFromClasspath("/org/elasticsearch/action/termvectors/multiRequest1.json");
        BytesReference bytes = new BytesArray(data);
        MultiTermVectorsRequest request = new MultiTermVectorsRequest();
        request.add(new TermVectorsRequest(), bytes);
        checkParsedParameters(request);
        
        data = Streams.copyToBytesFromClasspath("/org/elasticsearch/action/termvectors/multiRequest2.json");
        bytes = new BytesArray(data);
        request = new MultiTermVectorsRequest();
        request.add(new TermVectorsRequest(), bytes);
        checkParsedParameters(request);
        
    }
    void checkParsedParameters(MultiTermVectorsRequest request) {
        Set<String> ids = new HashSet<>();
        ids.add("1");
        ids.add("2");
        Set<String> fields = new HashSet<>();
        fields.add("a");
        fields.add("b");
        fields.add("c");
        for (TermVectorsRequest singleRequest : request.requests) {
            assertThat(singleRequest.index(), equalTo("testidx"));
            assertThat(singleRequest.type(), equalTo("test"));
            assertThat(singleRequest.payloads(), equalTo(false));
            assertThat(singleRequest.positions(), equalTo(false));
            assertThat(singleRequest.offsets(), equalTo(false));
            assertThat(singleRequest.termStatistics(), equalTo(true));
            assertThat(singleRequest.fieldStatistics(), equalTo(false));
            assertThat(singleRequest.id(),Matchers.anyOf(Matchers.equalTo("1"), Matchers.equalTo("2")));
            assertThat(singleRequest.selectedFields(), equalTo(fields));
        }
    }
    
}
