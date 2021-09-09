/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.termvectors;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.action.termvectors.TermVectorsRequest.Flag;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.action.document.RestTermVectorsAction;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TermVectorsUnitTests extends ESTestCase {
    public void testStreamResponse() throws Exception {
        TermVectorsResponse outResponse = new TermVectorsResponse("a", "c");
        outResponse.setExists(true);
        writeStandardTermVector(outResponse);

        // write
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        outResponse.writeTo(out);

        // read
        ByteArrayInputStream esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput esBuffer = new InputStreamStreamInput(esInBuffer);
        TermVectorsResponse inResponse = new TermVectorsResponse(esBuffer);

        // see if correct
        checkIfStandardTermVector(inResponse);

        outResponse = new TermVectorsResponse("a", "c");
        writeEmptyTermVector(outResponse);
        // write
        outBuffer = new ByteArrayOutputStream();
        out = new OutputStreamStreamOutput(outBuffer);
        outResponse.writeTo(out);

        // read
        esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        esBuffer = new InputStreamStreamInput(esInBuffer);
        inResponse = new TermVectorsResponse(esBuffer);
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

    public void testRestRequestParsing() throws Exception {
        BytesReference inputBytes = new BytesArray(
                " {\"fields\" : [\"a\",  \"b\",\"c\"], \"offsets\":false, \"positions\":false, \"payloads\":true}");

        TermVectorsRequest tvr = new TermVectorsRequest(null, null);
        XContentParser parser = createParser(JsonXContent.jsonXContent, inputBytes);
        TermVectorsRequest.parseRequest(tvr, parser, RestApiVersion.current());

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
        tvr = new TermVectorsRequest(null, null);
        parser = createParser(JsonXContent.jsonXContent, inputBytes);
        TermVectorsRequest.parseRequest(tvr, parser, RestApiVersion.current());
        additionalFields = "";
        RestTermVectorsAction.addFieldStringsFromParameter(tvr, additionalFields);
        assertThat(tvr.selectedFields(), equalTo(null));
        additionalFields = "b,c  ,d, e  ";
        RestTermVectorsAction.addFieldStringsFromParameter(tvr, additionalFields);
        assertThat(tvr.selectedFields().size(), equalTo(4));

    }

    public void testRequestParsingThrowsException() throws Exception {
        BytesReference inputBytes = new BytesArray(
                " {\"fields\" : \"a,  b,c   \", \"offsets\":false, \"positions\":false, \"payloads\":true, \"meaningless_term\":2}");
        TermVectorsRequest tvr = new TermVectorsRequest(null, null);
        boolean threwException = false;
        try {
            XContentParser parser = createParser(JsonXContent.jsonXContent, inputBytes);
            TermVectorsRequest.parseRequest(tvr, parser, RestApiVersion.current());
        } catch (Exception e) {
            threwException = true;
        }
        assertThat(threwException, equalTo(true));

    }

    public void testStreamRequest() throws IOException {
        for (int i = 0; i < 10; i++) {
            TermVectorsRequest request = new TermVectorsRequest("index", "id");
            request.offsets(random().nextBoolean());
            request.fieldStatistics(random().nextBoolean());
            request.payloads(random().nextBoolean());
            request.positions(random().nextBoolean());
            request.termStatistics(random().nextBoolean());
            String pref = random().nextBoolean() ? "somePreference" : null;
            request.preference(pref);
            request.doc(new BytesArray("{}"), randomBoolean(), XContentType.JSON);

            // write
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
            request.writeTo(out);

            // read
            ByteArrayInputStream esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
            InputStreamStreamInput esBuffer = new InputStreamStreamInput(esInBuffer);
            TermVectorsRequest req2 = new TermVectorsRequest(esBuffer);

            assertThat(request.offsets(), equalTo(req2.offsets()));
            assertThat(request.fieldStatistics(), equalTo(req2.fieldStatistics()));
            assertThat(request.payloads(), equalTo(req2.payloads()));
            assertThat(request.positions(), equalTo(req2.positions()));
            assertThat(request.termStatistics(), equalTo(req2.termStatistics()));
            assertThat(request.preference(), equalTo(pref));
            assertThat(request.routing(), equalTo(null));
            assertEquals(new BytesArray("{}"), request.doc());
            assertEquals(XContentType.JSON, request.xContentType());
        }
    }

    public void testStreamRequestLegacyVersion() throws IOException {
        for (int i = 0; i < 10; i++) {
            TermVectorsRequest request = new TermVectorsRequest("index", "id");
            request.offsets(random().nextBoolean());
            request.fieldStatistics(random().nextBoolean());
            request.payloads(random().nextBoolean());
            request.positions(random().nextBoolean());
            request.termStatistics(random().nextBoolean());
            String pref = random().nextBoolean() ? "somePreference" : null;
            request.preference(pref);
            request.doc(new BytesArray("{}"), randomBoolean(), XContentType.JSON);

            // write using older version which contains types
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
            out.setVersion(Version.V_7_2_0);
            request.writeTo(out);

            // First check the type on the stream was written as "_doc" by manually parsing the stream until the type
            ByteArrayInputStream esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
            InputStreamStreamInput esBuffer = new InputStreamStreamInput(esInBuffer);
            TaskId.readFromStream(esBuffer);
            if (esBuffer.readBoolean()) {
                new ShardId(esBuffer);
            }
            esBuffer.readOptionalString();
            assertThat(esBuffer.readString(), equalTo("_doc"));

            // now read the stream as normal to check it is parsed correct if received from an older node
            esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
            esBuffer = new InputStreamStreamInput(esInBuffer);
            esBuffer.setVersion(Version.V_7_2_0);
            TermVectorsRequest req2 = new TermVectorsRequest(esBuffer);

            assertThat(request.offsets(), equalTo(req2.offsets()));
            assertThat(request.fieldStatistics(), equalTo(req2.fieldStatistics()));
            assertThat(request.payloads(), equalTo(req2.payloads()));
            assertThat(request.positions(), equalTo(req2.positions()));
            assertThat(request.termStatistics(), equalTo(req2.termStatistics()));
            assertThat(request.preference(), equalTo(pref));
            assertThat(request.routing(), equalTo(null));
            assertEquals(new BytesArray("{}"), request.doc());
            assertEquals(XContentType.JSON, request.xContentType());
        }
    }

    public void testMultiParser() throws Exception {
        byte[] bytes = StreamsUtils.copyToBytesFromClasspath("/org/elasticsearch/action/termvectors/multiRequest1.json");
        XContentParser data = createParser(JsonXContent.jsonXContent, bytes);
        MultiTermVectorsRequest request = new MultiTermVectorsRequest();
        request.add(new TermVectorsRequest(), data);
        checkParsedParameters(request);

        bytes = StreamsUtils.copyToBytesFromClasspath("/org/elasticsearch/action/termvectors/multiRequest2.json");
        data = createParser(JsonXContent.jsonXContent, new BytesArray(bytes));
        request = new MultiTermVectorsRequest();
        request.add(new TermVectorsRequest(), data);

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
            assertThat(singleRequest.payloads(), equalTo(false));
            assertThat(singleRequest.positions(), equalTo(false));
            assertThat(singleRequest.offsets(), equalTo(false));
            assertThat(singleRequest.termStatistics(), equalTo(true));
            assertThat(singleRequest.fieldStatistics(), equalTo(false));
            assertThat(singleRequest.id(),Matchers.anyOf(Matchers.equalTo("1"), Matchers.equalTo("2")));
            assertThat(singleRequest.selectedFields(), equalTo(fields));
        }
    }

    // issue #12311
    public void testMultiParserFilter() throws Exception {
        byte[] bytes = StreamsUtils.copyToBytesFromClasspath("/org/elasticsearch/action/termvectors/multiRequest3.json");
        XContentParser data = createParser(JsonXContent.jsonXContent, bytes);
        MultiTermVectorsRequest request = new MultiTermVectorsRequest();
        request.add(new TermVectorsRequest(), data);

        checkParsedFilterParameters(request);
    }

    void checkParsedFilterParameters(MultiTermVectorsRequest multiRequest) {
        Set<String> ids = new HashSet<>(Arrays.asList("1", "2"));
        for (TermVectorsRequest request : multiRequest.requests) {
            assertThat(request.index(), equalTo("testidx"));
            assertTrue(ids.remove(request.id()));
            assertNotNull(request.filterSettings());
            assertThat(request.filterSettings().maxNumTerms, equalTo(20));
            assertThat(request.filterSettings().minTermFreq, equalTo(1));
            assertThat(request.filterSettings().maxTermFreq, equalTo(20));
            assertThat(request.filterSettings().minDocFreq, equalTo(1));
            assertThat(request.filterSettings().maxDocFreq, equalTo(20));
            assertThat(request.filterSettings().minWordLength, equalTo(1));
            assertThat(request.filterSettings().maxWordLength, equalTo(20));
        }
        assertTrue(ids.isEmpty());
    }
}
