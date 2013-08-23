/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.termvectors;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.termvector.TermVectorRequest;
import org.elasticsearch.action.termvector.TermVectorRequestBuilder;
import org.elasticsearch.action.termvector.TermVectorResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.TypeParsers;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.equalTo;

public class GetTermVectorTests extends AbstractTermVectorTests {



    @Test
    public void testNoSuchDoc() throws Exception {

        run(addMapping(prepareCreate("test"), "type1", new Object[]{"field", "type", "string", "term_vector",
                "with_positions_offsets_payloads"}));

        ensureYellow();

        client().prepareIndex("test", "type1", "666").setSource("field", "foo bar").execute().actionGet();
        refresh();
        for (int i = 0; i < 20; i++) {
            ActionFuture<TermVectorResponse> termVector = client().termVector(new TermVectorRequest("test", "type1", "" + i));
            TermVectorResponse actionGet = termVector.actionGet();
            assertThat(actionGet, Matchers.notNullValue());
            assertThat(actionGet.isExists(), Matchers.equalTo(false));

        }

    }
    
    @Test
    public void testExistingFieldWithNoTermVectorsNoNPE() throws Exception {

        run(addMapping(prepareCreate("test"), "type1", new Object[] { "existingfield", "type", "string", "term_vector",
                "with_positions_offsets_payloads" }));

        ensureYellow();
        // when indexing a field that simply has a question mark, the term
        // vectors will be null
        client().prepareIndex("test", "type1", "0").setSource("existingfield", "?").execute().actionGet();
        refresh();
        String[] selectedFields = { "existingfield" };
        ActionFuture<TermVectorResponse> termVector = client().termVector(
                new TermVectorRequest("test", "type1", "0").selectedFields(selectedFields));
        // lets see if the null term vectors are caught...
        termVector.actionGet();
        TermVectorResponse actionGet = termVector.actionGet();
        assertThat(actionGet.isExists(), Matchers.equalTo(true));

    }
    
    @Test
    public void testExistingFieldButNotInDocNPE() throws Exception {

        run(addMapping(prepareCreate("test"), "type1", new Object[] { "existingfield", "type", "string", "term_vector",
                "with_positions_offsets_payloads" }));

        ensureYellow();
        // when indexing a field that simply has a question mark, the term
        // vectors will be null
        client().prepareIndex("test", "type1", "0").setSource("anotherexistingfield", 1).execute().actionGet();
        refresh();
        String[] selectedFields = { "existingfield" };
        ActionFuture<TermVectorResponse> termVector = client().termVector(
                new TermVectorRequest("test", "type1", "0").selectedFields(selectedFields));
        // lets see if the null term vectors are caught...
        TermVectorResponse actionGet = termVector.actionGet();
        assertThat(actionGet.isExists(), Matchers.equalTo(true));

    }
    


    @Test
    public void testSimpleTermVectors() throws ElasticSearchException, IOException {

        run(addMapping(prepareCreate("test"), "type1",
                new Object[]{"field", "type", "string", "term_vector", "with_positions_offsets_payloads", "analyzer", "tv_test"})
                .setSettings(
                        ImmutableSettings.settingsBuilder().put("index.analysis.analyzer.tv_test.tokenizer", "whitespace")
                                .putArray("index.analysis.analyzer.tv_test.filter", "type_as_payload", "lowercase")));
        ensureYellow();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject().field("field", "the quick brown fox jumps over the lazy dog")
                            // 0the3 4quick9 10brown15 16fox19 20jumps25 26over30
                            // 31the34 35lazy39 40dog43
                            .endObject()).execute().actionGet();
            refresh();
        }
        String[] values = {"brown", "dog", "fox", "jumps", "lazy", "over", "quick", "the"};
        int[] freq = {1, 1, 1, 1, 1, 1, 1, 2};
        int[][] pos = {{2}, {8}, {3}, {4}, {7}, {5}, {1}, {0, 6}};
        int[][] startOffset = {{10}, {40}, {16}, {20}, {35}, {26}, {4}, {0, 31}};
        int[][] endOffset = {{15}, {43}, {19}, {25}, {39}, {30}, {9}, {3, 34}};
        for (int i = 0; i < 10; i++) {
            TermVectorRequestBuilder resp = client().prepareTermVector("test", "type1", Integer.toString(i)).setPayloads(true)
                    .setOffsets(true).setPositions(true).setSelectedFields();
            TermVectorResponse response = resp.execute().actionGet();
            assertThat("doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
            Fields fields = response.getFields();
            assertThat(fields.size(), equalTo(1));
            Terms terms = fields.terms("field");
            assertThat(terms.size(), equalTo(8l));
            TermsEnum iterator = terms.iterator(null);
            for (int j = 0; j < values.length; j++) {
                String string = values[j];
                BytesRef next = iterator.next();
                assertThat(next, Matchers.notNullValue());
                assertThat("expected " + string, string, equalTo(next.utf8ToString()));
                assertThat(next, Matchers.notNullValue());
                // do not test ttf or doc frequency, because here we have many
                // shards and do not know how documents are distributed
                DocsAndPositionsEnum docsAndPositions = iterator.docsAndPositions(null, null);
                assertThat(docsAndPositions.nextDoc(), equalTo(0));
                assertThat(freq[j], equalTo(docsAndPositions.freq()));
                int[] termPos = pos[j];
                int[] termStartOffset = startOffset[j];
                int[] termEndOffset = endOffset[j];
                assertThat(termPos.length, equalTo(freq[j]));
                assertThat(termStartOffset.length, equalTo(freq[j]));
                assertThat(termEndOffset.length, equalTo(freq[j]));
                for (int k = 0; k < freq[j]; k++) {
                    int nextPosition = docsAndPositions.nextPosition();
                    assertThat("term: " + string, nextPosition, equalTo(termPos[k]));
                    assertThat("term: " + string, docsAndPositions.startOffset(), equalTo(termStartOffset[k]));
                    assertThat("term: " + string, docsAndPositions.endOffset(), equalTo(termEndOffset[k]));
                    assertThat("term: " + string, docsAndPositions.getPayload(), equalTo(new BytesRef("word")));
                }
            }
            assertThat(iterator.next(), Matchers.nullValue());
        }
    }

    @Test
    public void testRandomSingleTermVectors() throws ElasticSearchException, IOException {
        FieldType ft = new FieldType();
        int config = randomInt(6);
        boolean storePositions = false;
        boolean storeOffsets = false;
        boolean storePayloads = false;
        boolean storeTermVectors = false;
        switch (config) {
            case 0: {
                // do nothing
            }
            case 1: {
                storeTermVectors = true;
            }
            case 2: {
                storeTermVectors = true;
                storePositions = true;
            }
            case 3: {
                storeTermVectors = true;
                storeOffsets = true;
            }
            case 4: {
                storeTermVectors = true;
                storePositions = true;
                storeOffsets = true;
            }
            case 5: {
                storeTermVectors = true;
                storePositions = true;
                storePayloads = true;
            }
            case 6: {
                storeTermVectors = true;
                storePositions = true;
                storeOffsets = true;
                storePayloads = true;
            }
        }
        ft.setStoreTermVectors(storeTermVectors);
        ft.setStoreTermVectorOffsets(storeOffsets);
        ft.setStoreTermVectorPayloads(storePayloads);
        ft.setStoreTermVectorPositions(storePositions);

        String optionString = AbstractFieldMapper.termVectorOptionsToString(ft);
        run(addMapping(prepareCreate("test"), "type1",
                new Object[]{"field", "type", "string", "term_vector", optionString, "analyzer", "tv_test"}).setSettings(
                ImmutableSettings.settingsBuilder().put("index.analysis.analyzer.tv_test.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.tv_test.filter", "type_as_payload", "lowercase")));
        ensureYellow();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject().field("field", "the quick brown fox jumps over the lazy dog")
                            // 0the3 4quick9 10brown15 16fox19 20jumps25 26over30
                            // 31the34 35lazy39 40dog43
                            .endObject()).execute().actionGet();
            refresh();
        }
        String[] values = {"brown", "dog", "fox", "jumps", "lazy", "over", "quick", "the"};
        int[] freq = {1, 1, 1, 1, 1, 1, 1, 2};
        int[][] pos = {{2}, {8}, {3}, {4}, {7}, {5}, {1}, {0, 6}};
        int[][] startOffset = {{10}, {40}, {16}, {20}, {35}, {26}, {4}, {0, 31}};
        int[][] endOffset = {{15}, {43}, {19}, {25}, {39}, {30}, {9}, {3, 34}};

        boolean isPayloadRequested = randomBoolean();
        boolean isOffsetRequested = randomBoolean();
        boolean isPositionsRequested = randomBoolean();
        String infoString = createInfoString(isPositionsRequested, isOffsetRequested, isPayloadRequested, optionString);
        for (int i = 0; i < 10; i++) {
            TermVectorRequestBuilder resp = client().prepareTermVector("test", "type1", Integer.toString(i))
                    .setPayloads(isPayloadRequested).setOffsets(isOffsetRequested).setPositions(isPositionsRequested).setSelectedFields();
            TermVectorResponse response = resp.execute().actionGet();
            assertThat(infoString + "doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
            Fields fields = response.getFields();
            assertThat(fields.size(), equalTo(ft.storeTermVectors() ? 1 : 0));
            if (ft.storeTermVectors()) {
                Terms terms = fields.terms("field");
                assertThat(terms.size(), equalTo(8l));
                TermsEnum iterator = terms.iterator(null);
                for (int j = 0; j < values.length; j++) {
                    String string = values[j];
                    BytesRef next = iterator.next();
                    assertThat(infoString, next, Matchers.notNullValue());
                    assertThat(infoString + "expected " + string, string, equalTo(next.utf8ToString()));
                    assertThat(infoString, next, Matchers.notNullValue());
                    // do not test ttf or doc frequency, because here we have
                    // many shards and do not know how documents are distributed
                    DocsAndPositionsEnum docsAndPositions = iterator.docsAndPositions(null, null);
                    // docs and pos only returns something if positions or
                    // payloads or offsets are stored / requestd Otherwise use
                    // DocsEnum?
                    assertThat(infoString, docsAndPositions.nextDoc(), equalTo(0));
                    assertThat(infoString, freq[j], equalTo(docsAndPositions.freq()));
                    int[] termPos = pos[j];
                    int[] termStartOffset = startOffset[j];
                    int[] termEndOffset = endOffset[j];
                    if (isPositionsRequested && storePositions) {
                        assertThat(infoString, termPos.length, equalTo(freq[j]));
                    }
                    if (isOffsetRequested && storeOffsets) {
                        assertThat(termStartOffset.length, equalTo(freq[j]));
                        assertThat(termEndOffset.length, equalTo(freq[j]));
                    }
                    for (int k = 0; k < freq[j]; k++) {
                        int nextPosition = docsAndPositions.nextPosition();
                        // only return something useful if requested and stored
                        if (isPositionsRequested && storePositions) {
                            assertThat(infoString + "positions for term: " + string, nextPosition, equalTo(termPos[k]));
                        } else {
                            assertThat(infoString + "positions for term: ", nextPosition, equalTo(-1));
                        }

                        // only return something useful if requested and stored
                        if (isPayloadRequested && storePayloads) {
                            assertThat(infoString + "payloads for term: " + string, docsAndPositions.getPayload(), equalTo(new BytesRef(
                                    "word")));
                        } else {
                            assertThat(infoString + "payloads for term: " + string, docsAndPositions.getPayload(), equalTo(null));
                        }
                        // only return something useful if requested and stored
                        if (isOffsetRequested && storeOffsets) {

                            assertThat(infoString + "startOffsets term: " + string, docsAndPositions.startOffset(),
                                    equalTo(termStartOffset[k]));
                            assertThat(infoString + "endOffsets term: " + string, docsAndPositions.endOffset(), equalTo(termEndOffset[k]));
                        } else {
                            assertThat(infoString + "startOffsets term: " + string, docsAndPositions.startOffset(), equalTo(-1));
                            assertThat(infoString + "endOffsets term: " + string, docsAndPositions.endOffset(), equalTo(-1));
                        }

                    }
                }
                assertThat(iterator.next(), Matchers.nullValue());
            }

        }
    }

    private String createInfoString(boolean isPositionsRequested, boolean isOffsetRequested, boolean isPayloadRequested,
                                    String optionString) {
        String ret = "Store config: " + optionString + "\n" + "Requested: pos-"
                + (isPositionsRequested ? "yes" : "no") + ", offsets-" + (isOffsetRequested ? "yes" : "no") + ", payload- "
                + (isPayloadRequested ? "yes" : "no") + "\n";
        return ret;
    }

    @Test
    public void testDuelESLucene() throws Exception {
        TestFieldSetting[] testFieldSettings = getFieldSettings();
        createIndexBasedOnFieldSettings(testFieldSettings, -1);
        TestDoc[] testDocs = generateTestDocs(5, testFieldSettings);

//        for (int i=0;i<testDocs.length;i++)
//            logger.info("Doc: {}",testDocs[i]);
        DirectoryReader directoryReader = indexDocsWithLucene(testDocs);
        TestConfig[] testConfigs = generateTestConfigs(20, testDocs, testFieldSettings);

        for (TestConfig test : testConfigs) {
            try {
                TermVectorRequestBuilder request = getRequestForConfig(test);
                if (test.expectedException != null) {
                    assertThrows(request, test.expectedException);
                    continue;
                }

                TermVectorResponse response = run(request);
                Fields luceneTermVectors = getTermVectorsFromLucene(directoryReader, test.doc);
                validateResponse(response, luceneTermVectors, test);
            } catch (Throwable t) {
                throw new Exception("Test exception while running " + test.toString(), t);
            }
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
        boolean excptiontrown = false;
        try {
            TypeParsers.parseTermVector("", ftOpts, builder);
        } catch (MapperParsingException e) {
            excptiontrown = true;
        }
        assertThat("TypeParsers.parseTermVector should accept string with_positions_payloads but does not.", excptiontrown, equalTo(false));

    }

    @Test
    public void testTermVectorStringGenerationIllegalState() throws Exception {
        FieldType ft = new FieldType();
        ft.setStoreTermVectorOffsets(true);
        ft.setStoreTermVectorPayloads(true);
        ft.setStoreTermVectors(true);
        ft.setStoreTermVectorPositions(false);
        String ftOpts = AbstractFieldMapper.termVectorOptionsToString(ft);
        assertThat(ftOpts, equalTo("with_offsets"));
    }

}
