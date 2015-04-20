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

import com.carrotsearch.hppc.ObjectIntOpenHashMap;

import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertThrows;
import static org.hamcrest.Matchers.*;

@Slow
public class GetTermVectorsTests extends AbstractTermVectorsTests {

    @Test
    public void testNoSuchDoc() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                        .startObject("field")
                            .field("type", "string")
                            .field("term_vector", "with_positions_offsets_payloads")
                        .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).addMapping("type1", mapping));

        ensureYellow();

        client().prepareIndex("test", "type1", "666").setSource("field", "foo bar").execute().actionGet();
        refresh();
        for (int i = 0; i < 20; i++) {
            ActionFuture<TermVectorsResponse> termVector = client().termVectors(new TermVectorsRequest(indexOrAlias(), "type1", "" + i));
            TermVectorsResponse actionGet = termVector.actionGet();
            assertThat(actionGet, notNullValue());
            assertThat(actionGet.getIndex(), equalTo("test"));
            assertThat(actionGet.isExists(), equalTo(false));
            // check response is nevertheless serializable to json
            actionGet.toXContent(jsonBuilder().startObject(), ToXContent.EMPTY_PARAMS);
        }
    }

    @Test
    public void testExistingFieldWithNoTermVectorsNoNPE() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                        .startObject("existingfield")
                            .field("type", "string")
                            .field("term_vector", "with_positions_offsets_payloads")
                        .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).addMapping("type1", mapping));

        ensureYellow();

        // when indexing a field that simply has a question mark, the term vectors will be null
        client().prepareIndex("test", "type1", "0").setSource("existingfield", "?").execute().actionGet();
        refresh();
        ActionFuture<TermVectorsResponse> termVector = client().termVectors(new TermVectorsRequest(indexOrAlias(), "type1", "0")
                .selectedFields(new String[]{"existingfield"}));

        // lets see if the null term vectors are caught...
        TermVectorsResponse actionGet = termVector.actionGet();
        assertThat(actionGet, notNullValue());
        assertThat(actionGet.isExists(), equalTo(true));
        assertThat(actionGet.getIndex(), equalTo("test"));
        assertThat(actionGet.getFields().terms("existingfield"), nullValue());
    }

    @Test
    public void testExistingFieldButNotInDocNPE() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                        .startObject("existingfield")
                            .field("type", "string")
                            .field("term_vector", "with_positions_offsets_payloads")
                        .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).addMapping("type1", mapping));

        ensureYellow();

        // when indexing a field that simply has a question mark, the term vectors will be null
        client().prepareIndex("test", "type1", "0").setSource("anotherexistingfield", 1).execute().actionGet();
        refresh();
        ActionFuture<TermVectorsResponse> termVectors = client().termVectors(new TermVectorsRequest(indexOrAlias(), "type1", "0")
                .selectedFields(randomBoolean() ? new String[]{"existingfield"} : null)
                .termStatistics(true)
                .fieldStatistics(true)
                .dfs(true));

        // lets see if the null term vectors are caught...
        TermVectorsResponse actionGet = termVectors.actionGet();
        assertThat(actionGet, notNullValue());
        assertThat(actionGet.isExists(), equalTo(true));
        assertThat(actionGet.getIndex(), equalTo("test"));
        assertThat(actionGet.getFields().terms("existingfield"), nullValue());
    }

    @Test
    public void testNotIndexedField() throws Exception {
        // must be of type string and indexed.
        assertAcked(prepareCreate("test")
                .addAlias(new Alias("alias"))
                .addMapping("type1",
                        "field0", "type=integer,", // no tvs
                        "field1", "type=string,index=no", // no tvs
                        "field2", "type=string,index=no,store=yes",  // no tvs
                        "field3", "type=string,index=no,term_vector=yes", // no tvs
                        "field4", "type=string,index=not_analyzed", // yes tvs
                        "field5", "type=string,index=analyzed")); // yes tvs

        ensureYellow();

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            indexBuilders.add(client().prepareIndex()
                    .setIndex("test")
                    .setType("type1")
                    .setId(String.valueOf(i))
                    .setSource("field" + i, i));
        }
        indexRandom(true, indexBuilders);

        for (int i = 0; i < 4; i++) {
            TermVectorsResponse resp = client().prepareTermVectors(indexOrAlias(), "type1", String.valueOf(i))
                    .setSelectedFields("field" + i)
                    .get();
            assertThat(resp, notNullValue());
            assertThat(resp.isExists(), equalTo(true));
            assertThat(resp.getIndex(), equalTo("test"));
            assertThat("field" + i + " :", resp.getFields().terms("field" + i), nullValue());
        }

        for (int i = 4; i < 6; i++) {
            TermVectorsResponse resp = client().prepareTermVectors(indexOrAlias(), "type1", String.valueOf(i))
                    .setSelectedFields("field" + i).get();
            assertThat(resp.getIndex(), equalTo("test"));
            assertThat("field" + i + " :", resp.getFields().terms("field" + i), notNullValue());
        }
    }

    @Test
    public void testSimpleTermVectors() throws ElasticsearchException, IOException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                        .startObject("field")
                            .field("type", "string")
                            .field("term_vector", "with_positions_offsets_payloads")
                            .field("analyzer", "tv_test")
                        .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", mapping)
                .addAlias(new Alias("alias"))
                .setSettings(settingsBuilder()
                        .put(indexSettings())
                        .put("index.analysis.analyzer.tv_test.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.tv_test.filter", "type_as_payload", "lowercase")));
        ensureYellow();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("field", "the quick brown fox jumps over the lazy dog")
                            // 0the3 4quick9 10brown15 16fox19 20jumps25 26over30
                            // 31the34 35lazy39 40dog43
                            .endObject()).execute().actionGet();
            refresh();
        }
        for (int i = 0; i < 10; i++) {
            TermVectorsRequestBuilder resp = client().prepareTermVectors(indexOrAlias(), "type1", Integer.toString(i)).setPayloads(true)
                    .setOffsets(true).setPositions(true).setSelectedFields();
            TermVectorsResponse response = resp.execute().actionGet();
            assertThat(response.getIndex(), equalTo("test"));
            assertThat("doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
            Fields fields = response.getFields();
            assertThat(fields.size(), equalTo(1));
            checkBrownFoxTermVector(fields, "field", true);
        }
    }

    @Test
    public void testRandomSingleTermVectors() throws ElasticsearchException, IOException {
        FieldType ft = new FieldType();
        int config = randomInt(6);
        boolean storePositions = false;
        boolean storeOffsets = false;
        boolean storePayloads = false;
        boolean storeTermVectors = false;
        switch (config) {
            case 0: {
                // do nothing
                break;
            }
            case 1: {
                storeTermVectors = true;
                break;
            }
            case 2: {
                storeTermVectors = true;
                storePositions = true;
                break;
            }
            case 3: {
                storeTermVectors = true;
                storeOffsets = true;
                break;
            }
            case 4: {
                storeTermVectors = true;
                storePositions = true;
                storeOffsets = true;
                break;
            }
            case 5: {
                storeTermVectors = true;
                storePositions = true;
                storePayloads = true;
                break;
            }
            case 6: {
                storeTermVectors = true;
                storePositions = true;
                storeOffsets = true;
                storePayloads = true;
                break;
            }
        }
        ft.setStoreTermVectors(storeTermVectors);
        ft.setStoreTermVectorOffsets(storeOffsets);
        ft.setStoreTermVectorPayloads(storePayloads);
        ft.setStoreTermVectorPositions(storePositions);

        String optionString = AbstractFieldMapper.termVectorOptionsToString(ft);
        XContentBuilder mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties")
                        .startObject("field")
                            .field("type", "string")
                            .field("term_vector", optionString)
                            .field("analyzer", "tv_test")
                        .endObject()
                .endObject()
                .endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", mapping)
                .setSettings(settingsBuilder()
                        .put("index.analysis.analyzer.tv_test.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.tv_test.filter", "type_as_payload", "lowercase")));
        ensureYellow();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("field", "the quick brown fox jumps over the lazy dog")
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
            TermVectorsRequestBuilder resp = client().prepareTermVectors("test", "type1", Integer.toString(i))
                    .setPayloads(isPayloadRequested).setOffsets(isOffsetRequested).setPositions(isPositionsRequested).setSelectedFields();
            TermVectorsResponse response = resp.execute().actionGet();
            assertThat(infoString + "doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
            Fields fields = response.getFields();
            assertThat(fields.size(), equalTo(ft.storeTermVectors() ? 1 : 0));
            if (ft.storeTermVectors()) {
                Terms terms = fields.terms("field");
                assertThat(terms.size(), equalTo(8l));
                TermsEnum iterator = terms.iterator();
                for (int j = 0; j < values.length; j++) {
                    String string = values[j];
                    BytesRef next = iterator.next();
                    assertThat(infoString, next, notNullValue());
                    assertThat(infoString + "expected " + string, string, equalTo(next.utf8ToString()));
                    assertThat(infoString, next, notNullValue());
                    // do not test ttf or doc frequency, because here we have
                    // many shards and do not know how documents are distributed
                    PostingsEnum docsAndPositions = iterator.postings(null, null, PostingsEnum.ALL);
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
                assertThat(iterator.next(), nullValue());
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
        createIndexBasedOnFieldSettings("test", "alias", testFieldSettings);
        //we generate as many docs as many shards we have
        TestDoc[] testDocs = generateTestDocs("test", testFieldSettings);

        DirectoryReader directoryReader = indexDocsWithLucene(testDocs);
        TestConfig[] testConfigs = generateTestConfigs(20, testDocs, testFieldSettings);

        for (TestConfig test : testConfigs) {
            try {
                TermVectorsRequestBuilder request = getRequestForConfig(test);
                if (test.expectedException != null) {
                    assertThrows(request, test.expectedException);
                    continue;
                }

                TermVectorsResponse response = request.get();
                Fields luceneTermVectors = getTermVectorsFromLucene(directoryReader, test.doc);
                validateResponse(response, luceneTermVectors, test);
            } catch (Throwable t) {
                throw new Exception("Test exception while running " + test.toString(), t);
            }
        }
    }

    @Test
    public void testRandomPayloadWithDelimitedPayloadTokenFilter() throws ElasticsearchException, IOException {
        //create the test document
        int encoding = randomIntBetween(0, 2);
        String encodingString = "";
        if (encoding == 0) {
            encodingString = "float";
        }
        if (encoding == 1) {
            encodingString = "int";
        }
        if (encoding == 2) {
            encodingString = "identity";
        }
        String[] tokens = crateRandomTokens();
        Map<String, List<BytesRef>> payloads = createPayloads(tokens, encoding);
        String delimiter = createRandomDelimiter(tokens);
        String queryString = createString(tokens, payloads, encoding, delimiter.charAt(0));
        //create the mapping
        XContentBuilder mapping = jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("field").field("type", "string").field("term_vector", "with_positions_offsets_payloads")
                .field("analyzer", "payload_test").endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", mapping).setSettings(
                settingsBuilder()
                        .put(indexSettings())
                        .put("index.analysis.analyzer.payload_test.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.payload_test.filter", "my_delimited_payload_filter")
                        .put("index.analysis.filter.my_delimited_payload_filter.delimiter", delimiter)
                        .put("index.analysis.filter.my_delimited_payload_filter.encoding", encodingString)
                        .put("index.analysis.filter.my_delimited_payload_filter.type", "delimited_payload_filter")));
        ensureYellow();

        client().prepareIndex("test", "type1", Integer.toString(1))
                .setSource(jsonBuilder().startObject().field("field", queryString).endObject()).execute().actionGet();
        refresh();
        TermVectorsRequestBuilder resp = client().prepareTermVectors("test", "type1", Integer.toString(1)).setPayloads(true).setOffsets(true)
                .setPositions(true).setSelectedFields();
        TermVectorsResponse response = resp.execute().actionGet();
        assertThat("doc id 1 doesn't exists but should", response.isExists(), equalTo(true));
        Fields fields = response.getFields();
        assertThat(fields.size(), equalTo(1));
        Terms terms = fields.terms("field");
        TermsEnum iterator = terms.iterator();
        while (iterator.next() != null) {
            String term = iterator.term().utf8ToString();
            PostingsEnum docsAndPositions = iterator.postings(null, null, PostingsEnum.ALL);
            assertThat(docsAndPositions.nextDoc(), equalTo(0));
            List<BytesRef> curPayloads = payloads.get(term);
            assertThat(term, curPayloads, notNullValue());
            assertNotNull(docsAndPositions);
            for (int k = 0; k < docsAndPositions.freq(); k++) {
                docsAndPositions.nextPosition();
                if (docsAndPositions.getPayload()!=null){
                    String infoString = "\nterm: " + term + " has payload \n"+ docsAndPositions.getPayload().toString() + "\n but should have payload \n"+curPayloads.get(k).toString();
                    assertThat(infoString, docsAndPositions.getPayload(), equalTo(curPayloads.get(k)));
                } else {
                    String infoString = "\nterm: " + term + " has no payload but should have payload \n"+curPayloads.get(k).toString();
                    assertThat(infoString, curPayloads.get(k).length, equalTo(0));
                }
            }
        }
        assertThat(iterator.next(), nullValue());
    }

    private String createRandomDelimiter(String[] tokens) {
        String delimiter = "";
        boolean isTokenOrWhitespace = true;
        while(isTokenOrWhitespace) {
            isTokenOrWhitespace = false;
            delimiter = randomUnicodeOfLength(1);
            for(String token:tokens) {
                if(token.contains(delimiter)) {
                    isTokenOrWhitespace = true;
                }
            }
            if(Character.isWhitespace(delimiter.charAt(0))) {
                isTokenOrWhitespace = true;
            }
        }
        return delimiter;
    }

    private String createString(String[] tokens, Map<String, List<BytesRef>> payloads, int encoding, char delimiter) {
        String resultString = "";
        ObjectIntOpenHashMap<String> payloadCounter = new ObjectIntOpenHashMap<>();
        for (String token : tokens) {
            if (!payloadCounter.containsKey(token)) {
                payloadCounter.putIfAbsent(token, 0);
            } else {
                payloadCounter.put(token, payloadCounter.get(token) + 1);
            }
            resultString = resultString + token;
            BytesRef payload = payloads.get(token).get(payloadCounter.get(token));
            if (payload.length > 0) {
                resultString = resultString + delimiter;
                switch (encoding) {
                case 0: {
                    resultString = resultString + Float.toString(PayloadHelper.decodeFloat(payload.bytes, payload.offset));
                    break;
                }
                case 1: {
                    resultString = resultString + Integer.toString(PayloadHelper.decodeInt(payload.bytes, payload.offset));
                    break;
                }
                case 2: {
                    resultString = resultString + payload.utf8ToString();
                    break;
                }
                default: {
                    throw new ElasticsearchException("unsupported encoding type");
                }
                }
            }
            resultString = resultString + " ";
        }
        return resultString;
    }

    private Map<String, List<BytesRef>> createPayloads(String[] tokens, int encoding) {
        Map<String, List<BytesRef>> payloads = new HashMap<>();
        for (String token : tokens) {
            if (payloads.get(token) == null) {
                payloads.put(token, new ArrayList<BytesRef>());
            }
            boolean createPayload = randomBoolean();
            if (createPayload) {
                switch (encoding) {
                case 0: {
                    float theFloat = randomFloat();
                    payloads.get(token).add(new BytesRef(PayloadHelper.encodeFloat(theFloat)));
                    break;
                }
                case 1: {
                    payloads.get(token).add(new BytesRef(PayloadHelper.encodeInt(randomInt())));
                    break;
                }
                case 2: {
                    String payload = randomUnicodeOfLengthBetween(50, 100);
                    for (int c = 0; c < payload.length(); c++) {
                        if (Character.isWhitespace(payload.charAt(c))) {
                            payload = payload.replace(payload.charAt(c), 'w');
                        }
                    }
                    payloads.get(token).add(new BytesRef(payload));
                    break;
                }
                default: {
                    throw new ElasticsearchException("unsupported encoding type");
                }
                }
            } else {
                payloads.get(token).add(new BytesRef());
            }
        }
        return payloads;
    }

    private String[] crateRandomTokens() {
        String[] tokens = { "the", "quick", "brown", "fox" };
        int numTokensWithDuplicates = randomIntBetween(3, 15);
        String[] finalTokens = new String[numTokensWithDuplicates];
        for (int i = 0; i < numTokensWithDuplicates; i++) {
            finalTokens[i] = tokens[randomIntBetween(0, tokens.length - 1)];
        }
        return finalTokens;
    }

    // like testSimpleTermVectors but we create fields with no term vectors
    @Test
    public void testSimpleTermVectorsWithGenerate() throws ElasticsearchException, IOException {
        String[] fieldNames = new String[10];
        for (int i = 0; i < fieldNames.length; i++) {
            fieldNames[i] = "field" + String.valueOf(i);
        }

        XContentBuilder mapping = jsonBuilder().startObject().startObject("type1").startObject("properties");
        XContentBuilder source = jsonBuilder().startObject();
        for (String field : fieldNames) {
            mapping.startObject(field)
                    .field("type", "string")
                    .field("term_vector", randomBoolean() ? "with_positions_offsets_payloads" : "no")
                    .field("analyzer", "tv_test")
                    .endObject();
            source.field(field, "the quick brown fox jumps over the lazy dog");
        }
        mapping.endObject().endObject().endObject();
        source.endObject();

        assertAcked(prepareCreate("test")
                .addMapping("type1", mapping)
                .setSettings(settingsBuilder()
                        .put(indexSettings())
                        .put("index.analysis.analyzer.tv_test.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.tv_test.filter", "type_as_payload", "lowercase")));

        ensureGreen();

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test", "type1", Integer.toString(i))
                    .setSource(source)
                    .execute().actionGet();
            refresh();
        }

        for (int i = 0; i < 10; i++) {
            TermVectorsResponse response = client().prepareTermVectors("test", "type1", Integer.toString(i))
                    .setPayloads(true)
                    .setOffsets(true)
                    .setPositions(true)
                    .setSelectedFields(fieldNames)
                    .execute().actionGet();
            assertThat("doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
            Fields fields = response.getFields();
            assertThat(fields.size(), equalTo(fieldNames.length));
            for (String fieldName : fieldNames) {
                // MemoryIndex does not support payloads
                checkBrownFoxTermVector(fields, fieldName, false);
            }
        }
    }

    private void checkBrownFoxTermVector(Fields fields, String fieldName, boolean withPayloads) throws ElasticsearchException, IOException {
        String[] values = {"brown", "dog", "fox", "jumps", "lazy", "over", "quick", "the"};
        int[] freq = {1, 1, 1, 1, 1, 1, 1, 2};
        int[][] pos = {{2}, {8}, {3}, {4}, {7}, {5}, {1}, {0, 6}};
        int[][] startOffset = {{10}, {40}, {16}, {20}, {35}, {26}, {4}, {0, 31}};
        int[][] endOffset = {{15}, {43}, {19}, {25}, {39}, {30}, {9}, {3, 34}};

        Terms terms = fields.terms(fieldName);
        assertThat(terms.size(), equalTo(8l));
        TermsEnum iterator = terms.iterator();
        for (int j = 0; j < values.length; j++) {
            String string = values[j];
            BytesRef next = iterator.next();
            assertThat(next, notNullValue());
            assertThat("expected " + string, string, equalTo(next.utf8ToString()));
            assertThat(next, notNullValue());
            // do not test ttf or doc frequency, because here we have many
            // shards and do not know how documents are distributed
            PostingsEnum docsAndPositions = iterator.postings(null, null, PostingsEnum.ALL);
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
                if (withPayloads) {
                    assertThat("term: " + string, docsAndPositions.getPayload(), equalTo(new BytesRef("word")));
                }
            }
        }
        assertThat(iterator.next(), nullValue());
    }

    @Test
    public void testDuelWithAndWithoutTermVectors() throws ElasticsearchException, IOException, ExecutionException, InterruptedException {
        // setup indices
        String[] indexNames = new String[] {"with_tv", "without_tv"};
        assertAcked(prepareCreate(indexNames[0])
                .addMapping("type1", "field1", "type=string,term_vector=with_positions_offsets,analyzer=keyword"));
        assertAcked(prepareCreate(indexNames[1])
                .addMapping("type1", "field1", "type=string,term_vector=no,analyzer=keyword"));
        ensureGreen();

        // index documents with and without term vectors
        String[] content = new String[]{
                "Generating a random permutation of a sequence (such as when shuffling cards).",
                "Selecting a random sample of a population (important in statistical sampling).",
                "Allocating experimental units via random assignment to a treatment or control condition.",
                "Generating random numbers: see Random number generation.",
                "Selecting a random sample of a population (important in statistical sampling).",
                "Allocating experimental units via random assignment to a treatment or control condition.",
                "Transforming a data stream (such as when using a scrambler in telecommunications)."};

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        for (String indexName : indexNames) {
            for (int id = 0; id < content.length; id++) {
                indexBuilders.add(client().prepareIndex()
                        .setIndex(indexName)
                        .setType("type1")
                        .setId(String.valueOf(id))
                        .setSource("field1", content[id]));
            }
        }
        indexRandom(true, indexBuilders);

        // request tvs and compare from each index
        for (int id = 0; id < content.length; id++) {
            Fields[] fields = new Fields[2];
            for (int j = 0; j < indexNames.length; j++) {
                TermVectorsResponse resp = client().prepareTermVector(indexNames[j], "type1", String.valueOf(id))
                        .setOffsets(true)
                        .setPositions(true)
                        .setSelectedFields("field1")
                        .get();
                assertThat("doc with index: " + indexNames[j] + ", type1 and id: " + id, resp.isExists(), equalTo(true));
                fields[j] = resp.getFields();
            }
            compareTermVectors("field1", fields[0], fields[1]);
        }
    }

    private void compareTermVectors(String fieldName, Fields fields0, Fields fields1) throws IOException {
        Terms terms0 = fields0.terms(fieldName);
        Terms terms1 = fields1.terms(fieldName);
        assertThat(terms0, notNullValue());
        assertThat(terms1, notNullValue());
        assertThat(terms0.size(), equalTo(terms1.size()));

        TermsEnum iter0 = terms0.iterator();
        TermsEnum iter1 = terms1.iterator();
        for (int i = 0; i < terms0.size(); i++) {
            BytesRef next0 = iter0.next();
            assertThat(next0, notNullValue());
            BytesRef next1 = iter1.next();
            assertThat(next1, notNullValue());

            // compare field value
            String string0 = next0.utf8ToString();
            String string1 = next1.utf8ToString();
            assertThat("expected: " + string0, string0, equalTo(string1));

            // compare df and ttf
            assertThat("term: " + string0, iter0.docFreq(), equalTo(iter1.docFreq()));
            assertThat("term: " + string0, iter0.totalTermFreq(), equalTo(iter1.totalTermFreq()));

            // compare freq and docs
            PostingsEnum docsAndPositions0 = iter0.postings(null, null, PostingsEnum.ALL);
            PostingsEnum docsAndPositions1 = iter1.postings(null, null, PostingsEnum.ALL);
            assertThat("term: " + string0, docsAndPositions0.nextDoc(), equalTo(docsAndPositions1.nextDoc()));
            assertThat("term: " + string0, docsAndPositions0.freq(), equalTo(docsAndPositions1.freq()));

            // compare position, start offsets and end offsets
            for (int j = 0; j < docsAndPositions0.freq(); j++) {
                assertThat("term: " + string0, docsAndPositions0.nextPosition(), equalTo(docsAndPositions1.nextPosition()));
                assertThat("term: " + string0, docsAndPositions0.startOffset(), equalTo(docsAndPositions1.startOffset()));
                assertThat("term: " + string0, docsAndPositions0.endOffset(), equalTo(docsAndPositions1.endOffset()));
            }
        }
        assertThat(iter0.next(), nullValue());
        assertThat(iter1.next(), nullValue());
    }

    @Test
    public void testSimpleWildCards() throws ElasticsearchException, IOException {
        int numFields = 25;

        XContentBuilder mapping = jsonBuilder().startObject().startObject("type1").startObject("properties");
        XContentBuilder source = jsonBuilder().startObject();
        for (int i = 0; i < numFields; i++) {
            mapping.startObject("field" + i)
                    .field("type", "string")
                    .field("term_vector", randomBoolean() ? "yes" : "no")
                    .endObject();
            source.field("field" + i, "some text here");
        }
        source.endObject();
        mapping.endObject().endObject().endObject();

        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).addMapping("type1", mapping));
        ensureGreen();

        client().prepareIndex("test", "type1", "0").setSource(source).get();
        refresh();

        TermVectorsResponse response = client().prepareTermVectors(indexOrAlias(), "type1", "0").setSelectedFields("field*").get();
        assertThat("Doc doesn't exists but should", response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat("All term vectors should have been generated", response.getFields().size(), equalTo(numFields));
    }

    @Test
    public void testArtificialVsExisting() throws ElasticsearchException, ExecutionException, InterruptedException, IOException {
        // setup indices
        ImmutableSettings.Builder settings = settingsBuilder()
                .put(indexSettings())
                .put("index.analysis.analyzer", "standard");
        assertAcked(prepareCreate("test")
                .setSettings(settings)
                .addMapping("type1", "field1", "type=string,term_vector=with_positions_offsets"));
        ensureGreen();

        // index documents existing document
        String[] content = new String[]{
                "Generating a random permutation of a sequence (such as when shuffling cards).",
                "Selecting a random sample of a population (important in statistical sampling).",
                "Allocating experimental units via random assignment to a treatment or control condition.",
                "Generating random numbers: see Random number generation."};

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        for (int i = 0; i < content.length; i++) {
            indexBuilders.add(client().prepareIndex()
                    .setIndex("test")
                    .setType("type1")
                    .setId(String.valueOf(i))
                    .setSource("field1", content[i]));
        }
        indexRandom(true, indexBuilders);

        for (int i = 0; i < content.length; i++) {
            // request tvs from existing document
            TermVectorsResponse respExisting = client().prepareTermVectors("test", "type1", String.valueOf(i))
                    .setOffsets(true)
                    .setPositions(true)
                    .setFieldStatistics(true)
                    .setTermStatistics(true)
                    .get();
            assertThat("doc with index: test, type1 and id: existing", respExisting.isExists(), equalTo(true));

            // request tvs from artificial document
            TermVectorsResponse respArtificial = client().prepareTermVectors()
                    .setIndex("test")
                    .setType("type1")
                    .setRouting(String.valueOf(i)) // ensure we get the stats from the same shard as existing doc
                    .setDoc(jsonBuilder()
                            .startObject()
                            .field("field1", content[i])
                            .endObject())
                    .setOffsets(true)
                    .setPositions(true)
                    .setFieldStatistics(true)
                    .setTermStatistics(true)
                    .get();
            assertThat("doc with index: test, type1 and id: " + String.valueOf(i), respArtificial.isExists(), equalTo(true));

            // compare existing tvs with artificial
            compareTermVectors("field1", respExisting.getFields(), respArtificial.getFields());
        }
    }

    @Test
    public void testArtificialNoDoc() throws IOException {
        // setup indices
        ImmutableSettings.Builder settings = settingsBuilder()
                .put(indexSettings())
                .put("index.analysis.analyzer", "standard");
        assertAcked(prepareCreate("test")
                .setSettings(settings)
                .addMapping("type1", "field1", "type=string"));
        ensureGreen();

        // request tvs from artificial document
        String text = "the quick brown fox jumps over the lazy dog";
        TermVectorsResponse resp = client().prepareTermVectors()
                .setIndex("test")
                .setType("type1")
                .setDoc(jsonBuilder()
                        .startObject()
                        .field("field1", text)
                        .endObject())
                .setOffsets(true)
                .setPositions(true)
                .setFieldStatistics(true)
                .setTermStatistics(true)
                .get();
        assertThat(resp.isExists(), equalTo(true));
        checkBrownFoxTermVector(resp.getFields(), "field1", false);
    }

    @Test
    public void testArtificialNonExistingField() throws Exception {
        // setup indices
        ImmutableSettings.Builder settings = settingsBuilder()
                .put(indexSettings())
                .put("index.analysis.analyzer", "standard");
        assertAcked(prepareCreate("test")
                .setSettings(settings)
                .addMapping("type1", "field1", "type=string"));
        ensureGreen();

        // index just one doc
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
            indexBuilders.add(client().prepareIndex()
                    .setIndex("test")
                    .setType("type1")
                    .setId("1")
                    .setRouting("1")
                    .setSource("field1", "some text"));
        indexRandom(true, indexBuilders);

        // request tvs from artificial document
        XContentBuilder doc = jsonBuilder()
                .startObject()
                    .field("field1", "the quick brown fox jumps over the lazy dog")
                    .field("non_existing", "the quick brown fox jumps over the lazy dog")
                .endObject();

        for (int i = 0; i < 2; i++) {
            TermVectorsResponse resp = client().prepareTermVectors()
                    .setIndex("test")
                    .setType("type1")
                    .setDoc(doc)
                    .setRouting("" + i)
                    .setOffsets(true)
                    .setPositions(true)
                    .setFieldStatistics(true)
                    .setTermStatistics(true)
                    .get();
            assertThat(resp.isExists(), equalTo(true));
            checkBrownFoxTermVector(resp.getFields(), "field1", false);
            // we should have created a mapping for this field
            waitForMappingOnMaster("test", "type1", "non_existing");
            // and return the generated term vectors
            checkBrownFoxTermVector(resp.getFields(), "non_existing", false);
        }
    }

    @Test
    public void testPerFieldAnalyzer() throws ElasticsearchException, IOException {
        int numFields = 25;

        // setup mapping and document source
        Set<String> withTermVectors = new HashSet<>();
        XContentBuilder mapping = jsonBuilder().startObject().startObject("type1").startObject("properties");
        XContentBuilder source = jsonBuilder().startObject();
        for (int i = 0; i < numFields; i++) {
            String fieldName = "field" + i;
            if (randomBoolean()) {
                withTermVectors.add(fieldName);
            }
            mapping.startObject(fieldName)
                    .field("type", "string")
                    .field("term_vector", withTermVectors.contains(fieldName) ? "yes" : "no")
                    .endObject();
            source.field(fieldName, "some text here");
        }
        source.endObject();
        mapping.endObject().endObject().endObject();

        // setup indices with mapping
        ImmutableSettings.Builder settings = settingsBuilder()
                .put(indexSettings())
                .put("index.analysis.analyzer", "standard");
        assertAcked(prepareCreate("test")
                .addAlias(new Alias("alias"))
                .setSettings(settings)
                .addMapping("type1", mapping));
        ensureGreen();

        // index a single document with prepared source
        client().prepareIndex("test", "type1", "0").setSource(source).get();
        refresh();

        // create random per_field_analyzer and selected fields
        Map<String, String> perFieldAnalyzer = new HashMap<>();
        Set<String> selectedFields = new HashSet<>();
        for (int i = 0; i < numFields; i++) {
            if (randomBoolean()) {
                perFieldAnalyzer.put("field" + i, "keyword");
            }
            if (randomBoolean()) {
                perFieldAnalyzer.put("non_existing" + i, "keyword");
            }
            if (randomBoolean()) {
                selectedFields.add("field" + i);
            }
            if (randomBoolean()) {
                selectedFields.add("non_existing" + i);
            }
        }

        // selected fields not specified
        TermVectorsResponse response = client().prepareTermVectors(indexOrAlias(), "type1", "0")
                .setPerFieldAnalyzer(perFieldAnalyzer)
                .get();

        // should return all fields that have terms vectors, some with overridden analyzer
        checkAnalyzedFields(response.getFields(), withTermVectors, perFieldAnalyzer);

        // selected fields specified including some not in the mapping
        response = client().prepareTermVectors(indexOrAlias(), "type1", "0")
                .setSelectedFields(selectedFields.toArray(Strings.EMPTY_ARRAY))
                .setPerFieldAnalyzer(perFieldAnalyzer)
                .get();

        // should return only the specified valid fields, with some with overridden analyzer
        checkAnalyzedFields(response.getFields(), selectedFields, perFieldAnalyzer);
    }

    private void checkAnalyzedFields(Fields fieldsObject, Set<String> fieldNames, Map<String, String> perFieldAnalyzer) throws IOException {
        Set<String> validFields = new HashSet<>();
        for (String fieldName : fieldNames){
            if (fieldName.startsWith("non_existing")) {
                assertThat("Non existing field\"" + fieldName + "\" should not be returned!", fieldsObject.terms(fieldName), nullValue());
                continue;
            }
            Terms terms = fieldsObject.terms(fieldName);
            assertThat("Existing field " + fieldName + "should have been returned", terms, notNullValue());
            // check overridden by keyword analyzer ...
            if (perFieldAnalyzer.containsKey(fieldName)) {
                TermsEnum iterator = terms.iterator();
                assertThat("Analyzer for " + fieldName + " should have been overridden!", iterator.next().utf8ToString(), equalTo("some text here"));
                assertThat(iterator.next(), nullValue());
            }
            validFields.add(fieldName);
        }
        // ensure no other fields are returned
        assertThat("More fields than expected are returned!", fieldsObject.size(), equalTo(validFields.size()));
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }

    @Test
    public void testDfs() throws ElasticsearchException, ExecutionException, InterruptedException, IOException {
        logger.info("Setting up the index ...");
        ImmutableSettings.Builder settings = settingsBuilder()
                .put(indexSettings())
                .put("index.analysis.analyzer", "standard")
                .put("index.number_of_shards", randomIntBetween(2, 10)); // we need at least 2 shards
        assertAcked(prepareCreate("test")
                .setSettings(settings)
                .addMapping("type1", "text", "type=string"));
        ensureGreen();

        int numDocs = scaledRandomIntBetween(25, 100);
        logger.info("Indexing {} documents...", numDocs);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("test", "type1", i + "").setSource("text", "cat"));
        }
        indexRandom(true, builders);

        XContentBuilder expectedStats = jsonBuilder()
                .startObject()
                .startObject("text")
                    .startObject("field_statistics")
                    .field("sum_doc_freq", numDocs)
                    .field("doc_count", numDocs)
                    .field("sum_ttf", numDocs)
                .endObject()
                    .startObject("terms")
                        .startObject("cat")
                        .field("doc_freq", numDocs)
                        .field("ttf", numDocs)
                        .endObject()
                    .endObject()
                .endObject()
                .endObject();

        logger.info("Without dfs 'cat' should appear strictly less than {} times.", numDocs);
        TermVectorsResponse response = client().prepareTermVectors("test", "type1", randomIntBetween(0, numDocs - 1) + "")
                .setSelectedFields("text")
                .setFieldStatistics(true)
                .setTermStatistics(true)
                .get();
        checkStats(response.getFields(), expectedStats, false);

        logger.info("With dfs 'cat' should appear exactly {} times.", numDocs);
        response = client().prepareTermVectors("test", "type1", randomIntBetween(0, numDocs - 1) + "")
                .setSelectedFields("text")
                .setFieldStatistics(true)
                .setTermStatistics(true)
                .setDfs(true)
                .get();
        checkStats(response.getFields(), expectedStats, true);
    }

    private void checkStats(Fields fields, XContentBuilder xContentBuilder, boolean isEqual) throws IOException {
        Map<String, Object> stats = JsonXContent.jsonXContent.createParser(xContentBuilder.bytes()).map();
        assertThat("number of fields expected:", fields.size(), equalTo(stats.size()));
        for (String fieldName : fields) {
            logger.info("Checking field statistics for field: {}", fieldName);
            Terms terms = fields.terms(fieldName);
            Map<String, Integer> fieldStatistics = getFieldStatistics(stats, fieldName);
            String msg = "field: " + fieldName + " ";
            assertThat(msg + "sum_doc_freq:",
                    (int) terms.getSumDocFreq(),
                    equalOrLessThanTo(fieldStatistics.get("sum_doc_freq"), isEqual));
            assertThat(msg + "doc_count:",
                    terms.getDocCount(),
                    equalOrLessThanTo(fieldStatistics.get("doc_count"), isEqual));
            assertThat(msg + "sum_ttf:",
                    (int) terms.getSumTotalTermFreq(),
                    equalOrLessThanTo(fieldStatistics.get("sum_ttf"), isEqual));

            final TermsEnum termsEnum = terms.iterator();
            BytesRef text;
            while((text = termsEnum.next()) != null) {
                String term = text.utf8ToString();
                logger.info("Checking term statistics for term: ({}, {})", fieldName, term);
                Map<String, Integer> termStatistics = getTermStatistics(stats, fieldName, term);
                msg = "term: (" + fieldName + "," + term + ") ";
                assertThat(msg + "doc_freq:",
                        termsEnum.docFreq(),
                        equalOrLessThanTo(termStatistics.get("doc_freq"), isEqual));
                assertThat(msg + "ttf:",
                        (int) termsEnum.totalTermFreq(),
                        equalOrLessThanTo(termStatistics.get("ttf"), isEqual));
            }
        }
    }

    private Map<String, Integer> getFieldStatistics(Map<String, Object> stats, String fieldName) throws IOException {
        return (Map<String, Integer>) ((Map<String, Object>) stats.get(fieldName)).get("field_statistics");
    }

    private Map<String, Integer> getTermStatistics(Map<String, Object> stats, String fieldName, String term) {
        return (Map<String, Integer>) ((Map<String, Object>) ((Map<String, Object>) stats.get(fieldName)).get("terms")).get(term);
    }

    private Matcher<Integer> equalOrLessThanTo(Integer value, boolean isEqual) {
        if (isEqual) {
            return equalTo(value);
        }
        return lessThan(value);
    }

    @Test
    public void testTermVectorsWithVersion() {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias"))
                .setSettings(ImmutableSettings.settingsBuilder().put("index.refresh_interval", -1)));
        ensureGreen();

        TermVectorsResponse response = client().prepareTermVectors("test", "type1", "1").get();
        assertThat(response.isExists(), equalTo(false));

        logger.info("--> index doc 1");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2").get();

        // From translog:

        // version 0 means ignore version, which is the default
        response = client().prepareTermVectors(indexOrAlias(), "type1", "1").setVersion(Versions.MATCH_ANY).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(1l));

        response = client().prepareTermVectors(indexOrAlias(), "type1", "1").setVersion(1).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(1l));

        try {
            client().prepareGet(indexOrAlias(), "type1", "1").setVersion(2).get();
            fail();
        } catch (VersionConflictEngineException e) {
            //all good
        }

        // From Lucene index:
        refresh();

        // version 0 means ignore version, which is the default
        response = client().prepareTermVectors(indexOrAlias(), "type1", "1").setVersion(Versions.MATCH_ANY).setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(1l));

        response = client().prepareTermVectors(indexOrAlias(), "type1", "1").setVersion(1).setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(1l));

        try {
            client().prepareGet(indexOrAlias(), "type1", "1").setVersion(2).setRealtime(false).get();
            fail();
        } catch (VersionConflictEngineException e) {
            //all good
        }

        logger.info("--> index doc 1 again, so increasing the version");
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2").get();

        // From translog:

        // version 0 means ignore version, which is the default
        response = client().prepareTermVectors(indexOrAlias(), "type1", "1").setVersion(Versions.MATCH_ANY).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2l));

        try {
            client().prepareGet(indexOrAlias(), "type1", "1").setVersion(1).get();
            fail();
        } catch (VersionConflictEngineException e) {
            //all good
        }

        response = client().prepareTermVectors(indexOrAlias(), "type1", "1").setVersion(2).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2l));

        // From Lucene index:
        refresh();

        // version 0 means ignore version, which is the default
        response = client().prepareTermVectors(indexOrAlias(), "type1", "1").setVersion(Versions.MATCH_ANY).setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2l));

        try {
            client().prepareGet(indexOrAlias(), "type1", "1").setVersion(1).setRealtime(false).get();
            fail();
        } catch (VersionConflictEngineException e) {
            //all good
        }

        response = client().prepareTermVectors(indexOrAlias(), "type1", "1").setVersion(2).setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2l));
    }

    @Test
    public void testFilterLength() throws ExecutionException, InterruptedException, IOException {
        logger.info("Setting up the index ...");
        ImmutableSettings.Builder settings = settingsBuilder()
                .put(indexSettings())
                .put("index.analysis.analyzer", "keyword");
        assertAcked(prepareCreate("test")
                .setSettings(settings)
                .addMapping("type1", "tags", "type=string"));
        ensureYellow();

        int numTerms = scaledRandomIntBetween(10, 50);
        logger.info("Indexing one document with tags of increasing length ...");
        List<String> tags = new ArrayList<>();
        for (int i = 0; i < numTerms; i++) {
            String tag = "a";
            for (int j = 0; j < i; j++) {
                tag += "a";
            }
            tags.add(tag);
        }
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("tags", tags));

        logger.info("Checking best tags by longest to shortest size ...");
        TermVectorsRequest.FilterSettings filterSettings = new TermVectorsRequest.FilterSettings();
        filterSettings.maxNumTerms = numTerms;
        TermVectorsResponse response;
        for (int i = 0; i < numTerms; i++) {
            filterSettings.minWordLength = numTerms - i;
            response = client().prepareTermVectors("test", "type1", "1")
                    .setSelectedFields("tags")
                    .setFieldStatistics(true)
                    .setTermStatistics(true)
                    .setFilterSettings(filterSettings)
                    .get();
            checkBestTerms(response.getFields().terms("tags"), tags.subList((numTerms - i - 1), numTerms));
        }
    }

    @Test
    public void testFilterTermFreq() throws ExecutionException, InterruptedException, IOException {
        logger.info("Setting up the index ...");
        ImmutableSettings.Builder settings = settingsBuilder()
                .put(indexSettings())
                .put("index.analysis.analyzer", "keyword");
        assertAcked(prepareCreate("test")
                .setSettings(settings)
                .addMapping("type1", "tags", "type=string"));
        ensureYellow();

        logger.info("Indexing one document with tags of increasing frequencies ...");
        int numTerms = scaledRandomIntBetween(10, 50);
        List<String> tags = new ArrayList<>();
        List<String> uniqueTags = new ArrayList<>();
        String tag;
        for (int i = 0; i < numTerms; i++) {
            tag = "tag_" + i;
            tags.add(tag);
            for (int j = 0; j < i; j++) {
                tags.add(tag);
            }
            uniqueTags.add(tag);
        }
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("tags", tags));

        logger.info("Checking best tags by highest to lowest term freq ...");
        TermVectorsRequest.FilterSettings filterSettings = new TermVectorsRequest.FilterSettings();
        TermVectorsResponse response;
        for (int i = 0; i < numTerms; i++) {
            filterSettings.maxNumTerms = i + 1;
            response = client().prepareTermVectors("test", "type1", "1")
                    .setSelectedFields("tags")
                    .setFieldStatistics(true)
                    .setTermStatistics(true)
                    .setFilterSettings(filterSettings)
                    .get();
            checkBestTerms(response.getFields().terms("tags"), uniqueTags.subList((numTerms - i - 1), numTerms));
        }
    }

    @Test
    public void testFilterDocFreq() throws ExecutionException, InterruptedException, IOException {
        logger.info("Setting up the index ...");
        ImmutableSettings.Builder settings = settingsBuilder()
                .put(indexSettings())
                .put("index.analysis.analyzer", "keyword")
                .put("index.number_of_shards", 1); // no dfs
        assertAcked(prepareCreate("test")
                .setSettings(settings)
                .addMapping("type1", "tags", "type=string"));
        ensureYellow();

        int numDocs = scaledRandomIntBetween(10, 50); // as many terms as there are docs
        logger.info("Indexing {} documents with tags of increasing dfs ...", numDocs);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        List<String> tags = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            tags.add("tag_" + i);
            builders.add(client().prepareIndex("test", "type1", i + "").setSource("tags", tags));
        }
        indexRandom(true, builders);

        logger.info("Checking best terms by highest to lowest idf ...");
        TermVectorsRequest.FilterSettings filterSettings = new TermVectorsRequest.FilterSettings();
        TermVectorsResponse response;
        for (int i = 0; i < numDocs; i++) {
            filterSettings.maxNumTerms = i + 1;
            response = client().prepareTermVectors("test", "type1", (numDocs - 1) + "")
                    .setSelectedFields("tags")
                    .setFieldStatistics(true)
                    .setTermStatistics(true)
                    .setFilterSettings(filterSettings)
                    .get();
            checkBestTerms(response.getFields().terms("tags"), tags.subList((numDocs - i - 1), numDocs));
        }
    }

    private void checkBestTerms(Terms terms, List<String> expectedTerms) throws IOException {
        final TermsEnum termsEnum = terms.iterator();
        List<String> bestTerms = new ArrayList<>();
        BytesRef text;
        while((text = termsEnum.next()) != null) {
            bestTerms.add(text.utf8ToString());
        }
        Collections.sort(expectedTerms);
        Collections.sort(bestTerms);
        assertArrayEquals(expectedTerms.toArray(), bestTerms.toArray());
    }
}
