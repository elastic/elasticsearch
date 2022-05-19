/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.termvectors;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.MockKeywordPlugin;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertRequestBuilderThrows;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GetTermVectorsIT extends AbstractTermVectorsTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MockKeywordPlugin.class);
    }

    public void testNoSuchDoc() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setMapping(mapping));

        client().prepareIndex("test").setId("666").setSource("field", "foo bar").execute().actionGet();
        refresh();
        for (int i = 0; i < 20; i++) {
            ActionFuture<TermVectorsResponse> termVector = client().termVectors(new TermVectorsRequest(indexOrAlias(), "" + i));
            TermVectorsResponse actionGet = termVector.actionGet();
            assertThat(actionGet, notNullValue());
            assertThat(actionGet.getIndex(), equalTo("test"));
            assertThat(actionGet.isExists(), equalTo(false));
            // check response is nevertheless serializable to json
            actionGet.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS);
        }
    }

    public void testExistingFieldWithNoTermVectorsNoNPE() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("existingfield")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setMapping(mapping));

        // when indexing a field that simply has a question mark, the term vectors will be null
        client().prepareIndex("test").setId("0").setSource("existingfield", "?").execute().actionGet();
        refresh();
        ActionFuture<TermVectorsResponse> termVector = client().termVectors(
            new TermVectorsRequest(indexOrAlias(), "0").selectedFields(new String[] { "existingfield" })
        );

        // lets see if the null term vectors are caught...
        TermVectorsResponse actionGet = termVector.actionGet();
        assertThat(actionGet, notNullValue());
        assertThat(actionGet.isExists(), equalTo(true));
        assertThat(actionGet.getIndex(), equalTo("test"));
        assertThat(actionGet.getFields().terms("existingfield"), nullValue());
    }

    public void testExistingFieldButNotInDocNPE() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("existingfield")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setMapping(mapping));

        // when indexing a field that simply has a question mark, the term vectors will be null
        client().prepareIndex("test").setId("0").setSource("anotherexistingfield", 1).execute().actionGet();
        refresh();
        ActionFuture<TermVectorsResponse> termVectors = client().termVectors(
            new TermVectorsRequest(indexOrAlias(), "0").selectedFields(randomBoolean() ? new String[] { "existingfield" } : null)
                .termStatistics(true)
                .fieldStatistics(true)
        );

        // lets see if the null term vectors are caught...
        TermVectorsResponse actionGet = termVectors.actionGet();
        assertThat(actionGet, notNullValue());
        assertThat(actionGet.isExists(), equalTo(true));
        assertThat(actionGet.getIndex(), equalTo("test"));
        assertThat(actionGet.getFields().terms("existingfield"), nullValue());
    }

    public void testNotIndexedField() throws Exception {
        // must be of type string and indexed.
        assertAcked(
            prepareCreate("test").addAlias(new Alias("alias"))
                .setMapping(
                    "field0",
                    "type=integer,", // no tvs
                    "field1",
                    "type=text,index=false", // no tvs
                    "field2",
                    "type=text,index=false,store=true",  // no tvs
                    "field3",
                    "type=text,index=false,term_vector=yes", // no tvs
                    "field4",
                    "type=keyword", // yes tvs
                    "field5",
                    "type=text,index=true"
                )
        ); // yes tvs

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            indexBuilders.add(client().prepareIndex().setIndex("test").setId(String.valueOf(i)).setSource("field" + i, i));
        }
        indexRandom(true, indexBuilders);

        for (int i = 0; i < 4; i++) {
            TermVectorsResponse resp = client().prepareTermVectors(indexOrAlias(), String.valueOf(i)).setSelectedFields("field" + i).get();
            assertThat(resp, notNullValue());
            assertThat(resp.isExists(), equalTo(true));
            assertThat(resp.getIndex(), equalTo("test"));
            assertThat("field" + i + " :", resp.getFields().terms("field" + i), nullValue());
        }

        for (int i = 4; i < 6; i++) {
            TermVectorsResponse resp = client().prepareTermVectors(indexOrAlias(), String.valueOf(i)).setSelectedFields("field" + i).get();
            assertThat(resp.getIndex(), equalTo("test"));
            assertThat("field" + i + " :", resp.getFields().terms("field" + i), notNullValue());
        }
    }

    public void testSimpleTermVectors() throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .field("analyzer", "tv_test")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            prepareCreate("test").setMapping(mapping)
                .addAlias(new Alias("alias"))
                .setSettings(
                    Settings.builder()
                        .put(indexSettings())
                        .put("index.analysis.analyzer.tv_test.tokenizer", "standard")
                        .putList("index.analysis.analyzer.tv_test.filter", "lowercase")
                )
        );
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test")
                .setId(Integer.toString(i))
                .setSource(
                    jsonBuilder().startObject()
                        .field("field", "the quick brown fox jumps over the lazy dog")
                        // 0the3 4quick9 10brown15 16fox19 20jumps25 26over30
                        // 31the34 35lazy39 40dog43
                        .endObject()
                )
                .execute()
                .actionGet();
            refresh();
        }
        for (int i = 0; i < 10; i++) {
            TermVectorsRequestBuilder resp = client().prepareTermVectors(indexOrAlias(), Integer.toString(i))
                .setPayloads(true)
                .setOffsets(true)
                .setPositions(true)
                .setSelectedFields();
            TermVectorsResponse response = resp.execute().actionGet();
            assertThat(response.getIndex(), equalTo("test"));
            assertThat("doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
            Fields fields = response.getFields();
            assertThat(fields.size(), equalTo(1));
            checkBrownFoxTermVector(fields, "field", true);
        }
    }

    public static String termVectorOptionsToString(FieldType fieldType) {
        if (fieldType.storeTermVectors() == false) {
            return "no";
        } else if (fieldType.storeTermVectorOffsets() == false && fieldType.storeTermVectorPositions() == false) {
            return "yes";
        } else if (fieldType.storeTermVectorOffsets() && fieldType.storeTermVectorPositions() == false) {
            return "with_offsets";
        } else {
            StringBuilder builder = new StringBuilder("with");
            if (fieldType.storeTermVectorPositions()) {
                builder.append("_positions");
            }
            if (fieldType.storeTermVectorOffsets()) {
                builder.append("_offsets");
            }
            if (fieldType.storeTermVectorPayloads()) {
                builder.append("_payloads");
            }
            return builder.toString();
        }
    }

    public void testRandomSingleTermVectors() throws IOException {
        FieldType ft = new FieldType();
        int config = randomInt(4);
        boolean storePositions = false;
        boolean storeOffsets = false;
        boolean storeTermVectors = false;
        switch (config) {
            case 0 -> {
                // do nothing
            }
            case 1 -> {
                storeTermVectors = true;
            }
            case 2 -> {
                storeTermVectors = true;
                storePositions = true;
            }
            case 3 -> {
                storeTermVectors = true;
                storeOffsets = true;
            }
            case 4 -> {
                storeTermVectors = true;
                storePositions = true;
                storeOffsets = true;
            }
            default -> throw new IllegalArgumentException("Unsupported option: " + config);
        }
        ft.setStoreTermVectors(storeTermVectors);
        ft.setStoreTermVectorOffsets(storeOffsets);
        ft.setStoreTermVectorPositions(storePositions);

        String optionString = termVectorOptionsToString(ft);
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("term_vector", optionString)
            .field("analyzer", "tv_test")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            prepareCreate("test").setMapping(mapping)
                .setSettings(
                    Settings.builder()
                        .put("index.analysis.analyzer.tv_test.tokenizer", "standard")
                        .putList("index.analysis.analyzer.tv_test.filter", "lowercase")
                )
        );
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test")
                .setId(Integer.toString(i))
                .setSource(
                    jsonBuilder().startObject()
                        .field("field", "the quick brown fox jumps over the lazy dog")
                        // 0the3 4quick9 10brown15 16fox19 20jumps25 26over30
                        // 31the34 35lazy39 40dog43
                        .endObject()
                )
                .execute()
                .actionGet();
            refresh();
        }
        String[] values = { "brown", "dog", "fox", "jumps", "lazy", "over", "quick", "the" };
        int[] freq = { 1, 1, 1, 1, 1, 1, 1, 2 };
        int[][] pos = { { 2 }, { 8 }, { 3 }, { 4 }, { 7 }, { 5 }, { 1 }, { 0, 6 } };
        int[][] startOffset = { { 10 }, { 40 }, { 16 }, { 20 }, { 35 }, { 26 }, { 4 }, { 0, 31 } };
        int[][] endOffset = { { 15 }, { 43 }, { 19 }, { 25 }, { 39 }, { 30 }, { 9 }, { 3, 34 } };

        boolean isOffsetRequested = randomBoolean();
        boolean isPositionsRequested = randomBoolean();
        String infoString = createInfoString(isPositionsRequested, isOffsetRequested, optionString);
        for (int i = 0; i < 10; i++) {
            TermVectorsRequestBuilder resp = client().prepareTermVectors("test", Integer.toString(i))
                .setOffsets(isOffsetRequested)
                .setPositions(isPositionsRequested)
                .setSelectedFields();
            TermVectorsResponse response = resp.execute().actionGet();
            assertThat(infoString + "doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
            Fields fields = response.getFields();
            assertThat(fields.size(), equalTo(ft.storeTermVectors() ? 1 : 0));
            if (ft.storeTermVectors()) {
                Terms terms = fields.terms("field");
                assertThat(terms.size(), equalTo(8L));
                TermsEnum iterator = terms.iterator();
                for (int j = 0; j < values.length; j++) {
                    String string = values[j];
                    BytesRef next = iterator.next();
                    assertThat(infoString, next, notNullValue());
                    assertThat(infoString + "expected " + string, string, equalTo(next.utf8ToString()));
                    assertThat(infoString, next, notNullValue());
                    // do not test ttf or doc frequency, because here we have
                    // many shards and do not know how documents are distributed
                    PostingsEnum docsAndPositions = iterator.postings(null, PostingsEnum.ALL);
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
                        // payloads are never made by the mapping in this test
                        assertNull(infoString + "payloads for term: " + string, docsAndPositions.getPayload());
                        // only return something useful if requested and stored
                        if (isOffsetRequested && storeOffsets) {

                            assertThat(
                                infoString + "startOffsets term: " + string,
                                docsAndPositions.startOffset(),
                                equalTo(termStartOffset[k])
                            );
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

    private String createInfoString(boolean isPositionsRequested, boolean isOffsetRequested, String optionString) {
        String ret = "Store config: "
            + optionString
            + "\n"
            + "Requested: pos-"
            + (isPositionsRequested ? "yes" : "no")
            + ", offsets-"
            + (isOffsetRequested ? "yes" : "no")
            + "\n";
        return ret;
    }

    public void testDuelESLucene() throws Exception {
        TestFieldSetting[] testFieldSettings = getFieldSettings();
        createIndexBasedOnFieldSettings("test", "alias", testFieldSettings);
        // we generate as many docs as many shards we have
        TestDoc[] testDocs = generateTestDocs("test", testFieldSettings);

        DirectoryReader directoryReader = indexDocsWithLucene(testDocs);
        TestConfig[] testConfigs = generateTestConfigs(20, testDocs, testFieldSettings);

        for (TestConfig test : testConfigs) {
            TermVectorsRequestBuilder request = getRequestForConfig(test);
            if (test.expectedException != null) {
                assertRequestBuilderThrows(request, test.expectedException);
                continue;
            }

            TermVectorsResponse response = request.get();
            Fields luceneTermVectors = getTermVectorsFromLucene(directoryReader, test.doc);
            validateResponse(response, luceneTermVectors, test);
        }
    }

    // like testSimpleTermVectors but we create fields with no term vectors
    public void testSimpleTermVectorsWithGenerate() throws IOException {
        String[] fieldNames = new String[10];
        for (int i = 0; i < fieldNames.length; i++) {
            fieldNames[i] = "field" + String.valueOf(i);
        }

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");
        XContentBuilder source = jsonBuilder().startObject();
        for (String field : fieldNames) {
            mapping.startObject(field)
                .field("type", "text")
                .field("term_vector", randomBoolean() ? "with_positions_offsets_payloads" : "no")
                .field("analyzer", "tv_test")
                .endObject();
            source.field(field, "the quick brown fox jumps over the lazy dog");
        }
        mapping.endObject().endObject().endObject();
        source.endObject();

        assertAcked(
            prepareCreate("test").setMapping(mapping)
                .setSettings(
                    Settings.builder()
                        .put(indexSettings())
                        .put("index.analysis.analyzer.tv_test.tokenizer", "standard")
                        .putList("index.analysis.analyzer.tv_test.filter", "lowercase")
                )
        );

        ensureGreen();

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource(source).execute().actionGet();
            refresh();
        }

        for (int i = 0; i < 10; i++) {
            TermVectorsResponse response = client().prepareTermVectors("test", Integer.toString(i))
                .setPayloads(true)
                .setOffsets(true)
                .setPositions(true)
                .setSelectedFields(fieldNames)
                .execute()
                .actionGet();
            assertThat("doc id: " + i + " doesn't exists but should", response.isExists(), equalTo(true));
            Fields fields = response.getFields();
            assertThat(fields.size(), equalTo(fieldNames.length));
            for (String fieldName : fieldNames) {
                // MemoryIndex does not support payloads
                checkBrownFoxTermVector(fields, fieldName, false);
            }
        }
    }

    private void checkBrownFoxTermVector(Fields fields, String fieldName, boolean withPayloads) throws IOException {
        String[] values = { "brown", "dog", "fox", "jumps", "lazy", "over", "quick", "the" };
        int[] freq = { 1, 1, 1, 1, 1, 1, 1, 2 };
        int[][] pos = { { 2 }, { 8 }, { 3 }, { 4 }, { 7 }, { 5 }, { 1 }, { 0, 6 } };
        int[][] startOffset = { { 10 }, { 40 }, { 16 }, { 20 }, { 35 }, { 26 }, { 4 }, { 0, 31 } };
        int[][] endOffset = { { 15 }, { 43 }, { 19 }, { 25 }, { 39 }, { 30 }, { 9 }, { 3, 34 } };

        Terms terms = fields.terms(fieldName);
        assertThat(terms.size(), equalTo(8L));
        TermsEnum iterator = terms.iterator();
        for (int j = 0; j < values.length; j++) {
            String string = values[j];
            BytesRef next = iterator.next();
            assertThat(next, notNullValue());
            assertThat("expected " + string, string, equalTo(next.utf8ToString()));
            assertThat(next, notNullValue());
            // do not test ttf or doc frequency, because here we have many
            // shards and do not know how documents are distributed
            PostingsEnum docsAndPositions = iterator.postings(null, PostingsEnum.ALL);
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
                // We never configure an analyzer with payloads for this test so this is never returned
                assertNull("term: " + string, docsAndPositions.getPayload());
            }
        }
        assertThat(iterator.next(), nullValue());
    }

    public void testDuelWithAndWithoutTermVectors() throws IOException, ExecutionException, InterruptedException {
        // setup indices
        String[] indexNames = new String[] { "with_tv", "without_tv" };
        assertAcked(prepareCreate(indexNames[0]).setMapping("field1", "type=text,term_vector=with_positions_offsets,analyzer=keyword"));
        assertAcked(prepareCreate(indexNames[1]).setMapping("field1", "type=text,term_vector=no,analyzer=keyword"));
        ensureGreen();

        // index documents with and without term vectors
        String[] content = new String[] {
            "Generating a random permutation of a sequence (such as when shuffling cards).",
            "Selecting a random sample of a population (important in statistical sampling).",
            "Allocating experimental units via random assignment to a treatment or control condition.",
            "Generating random numbers: see Random number generation.",
            "Selecting a random sample of a population (important in statistical sampling).",
            "Allocating experimental units via random assignment to a treatment or control condition.",
            "Transforming a data stream (such as when using a scrambler in telecommunications)." };

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        for (String indexName : indexNames) {
            for (int id = 0; id < content.length; id++) {
                indexBuilders.add(client().prepareIndex().setIndex(indexName).setId(String.valueOf(id)).setSource("field1", content[id]));
            }
        }
        indexRandom(true, indexBuilders);

        // request tvs and compare from each index
        for (int id = 0; id < content.length; id++) {
            Fields[] fields = new Fields[2];
            for (int j = 0; j < indexNames.length; j++) {
                TermVectorsResponse resp = client().prepareTermVectors(indexNames[j], String.valueOf(id))
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
            PostingsEnum docsAndPositions0 = iter0.postings(null, PostingsEnum.ALL);
            PostingsEnum docsAndPositions1 = iter1.postings(null, PostingsEnum.ALL);
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

    public void testSimpleWildCards() throws IOException {
        int numFields = 25;

        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");
        XContentBuilder source = jsonBuilder().startObject();
        for (int i = 0; i < numFields; i++) {
            mapping.startObject("field" + i).field("type", "text").field("term_vector", randomBoolean() ? "yes" : "no").endObject();
            source.field("field" + i, "some text here");
        }
        source.endObject();
        mapping.endObject().endObject().endObject();

        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setMapping(mapping));
        ensureGreen();

        client().prepareIndex("test").setId("0").setSource(source).get();
        refresh();

        TermVectorsResponse response = client().prepareTermVectors(indexOrAlias(), "0").setSelectedFields("field*").get();
        assertThat("Doc doesn't exists but should", response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat("All term vectors should have been generated", response.getFields().size(), equalTo(numFields));
    }

    public void testArtificialVsExisting() throws ExecutionException, InterruptedException, IOException {
        // setup indices
        Settings.Builder settings = Settings.builder().put(indexSettings()).put("index.analysis.analyzer", "standard");
        assertAcked(prepareCreate("test").setSettings(settings).setMapping("field1", "type=text,term_vector=with_positions_offsets"));
        ensureGreen();

        // index documents existing document
        String[] content = new String[] {
            "Generating a random permutation of a sequence (such as when shuffling cards).",
            "Selecting a random sample of a population (important in statistical sampling).",
            "Allocating experimental units via random assignment to a treatment or control condition.",
            "Generating random numbers: see Random number generation." };

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        for (int i = 0; i < content.length; i++) {
            indexBuilders.add(client().prepareIndex().setIndex("test").setId(String.valueOf(i)).setSource("field1", content[i]));
        }
        indexRandom(true, indexBuilders);

        for (int i = 0; i < content.length; i++) {
            // request tvs from existing document
            TermVectorsResponse respExisting = client().prepareTermVectors("test", String.valueOf(i))
                .setOffsets(true)
                .setPositions(true)
                .setFieldStatistics(true)
                .setTermStatistics(true)
                .get();
            assertThat("doc with index: test, type1 and id: existing", respExisting.isExists(), equalTo(true));

            // request tvs from artificial document
            TermVectorsResponse respArtificial = client().prepareTermVectors()
                .setIndex("test")
                .setRouting(String.valueOf(i)) // ensure we get the stats from the same shard as existing doc
                .setDoc(jsonBuilder().startObject().field("field1", content[i]).endObject())
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

    public void testArtificialNoDoc() throws IOException {
        // setup indices
        Settings.Builder settings = Settings.builder().put(indexSettings()).put("index.analysis.analyzer", "standard");
        assertAcked(prepareCreate("test").setSettings(settings).setMapping("field1", "type=text"));
        ensureGreen();

        // request tvs from artificial document
        String text = "the quick brown fox jumps over the lazy dog";
        TermVectorsResponse resp = client().prepareTermVectors()
            .setIndex("test")
            .setDoc(jsonBuilder().startObject().field("field1", text).endObject())
            .setOffsets(true)
            .setPositions(true)
            .setFieldStatistics(true)
            .setTermStatistics(true)
            .get();
        assertThat(resp.isExists(), equalTo(true));
        checkBrownFoxTermVector(resp.getFields(), "field1", false);

        // Since the index is empty, all of artificial document's "term_statistics" should be 0/absent
        Terms terms = resp.getFields().terms("field1");
        assertEquals("sumDocFreq should be 0 for a non-existing field!", 0, terms.getSumDocFreq());
        assertEquals("sumTotalTermFreq should be 0 for a non-existing field!", 0, terms.getSumTotalTermFreq());
        TermsEnum termsEnum = terms.iterator(); // we're guaranteed to receive terms for that field
        while (termsEnum.next() != null) {
            String term = termsEnum.term().utf8ToString();
            assertEquals("term [" + term + "] does not exist in the index; ttf should be 0!", 0, termsEnum.totalTermFreq());
        }
    }

    public void testPerFieldAnalyzer() throws IOException {
        int numFields = 25;

        // setup mapping and document source
        Set<String> withTermVectors = new HashSet<>();
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties");
        XContentBuilder source = jsonBuilder().startObject();
        for (int i = 0; i < numFields; i++) {
            String fieldName = "field" + i;
            if (randomBoolean()) {
                withTermVectors.add(fieldName);
            }
            mapping.startObject(fieldName)
                .field("type", "text")
                .field("term_vector", withTermVectors.contains(fieldName) ? "yes" : "no")
                .endObject();
            source.field(fieldName, "some text here");
        }
        source.endObject();
        mapping.endObject().endObject().endObject();

        // setup indices with mapping
        Settings.Builder settings = Settings.builder().put(indexSettings()).put("index.analysis.analyzer", "standard");
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSettings(settings).setMapping(mapping));
        ensureGreen();

        // index a single document with prepared source
        client().prepareIndex("test").setId("0").setSource(source).get();
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
        TermVectorsResponse response = client().prepareTermVectors(indexOrAlias(), "0").setPerFieldAnalyzer(perFieldAnalyzer).get();

        // should return all fields that have terms vectors, some with overridden analyzer
        checkAnalyzedFields(response.getFields(), withTermVectors, perFieldAnalyzer);

        // selected fields specified including some not in the mapping
        response = client().prepareTermVectors(indexOrAlias(), "0")
            .setSelectedFields(selectedFields.toArray(Strings.EMPTY_ARRAY))
            .setPerFieldAnalyzer(perFieldAnalyzer)
            .get();

        // should return only the specified valid fields, with some with overridden analyzer
        checkAnalyzedFields(response.getFields(), selectedFields, perFieldAnalyzer);
    }

    private void checkAnalyzedFields(Fields fieldsObject, Set<String> fieldNames, Map<String, String> perFieldAnalyzer) throws IOException {
        Set<String> validFields = new HashSet<>();
        for (String fieldName : fieldNames) {
            if (fieldName.startsWith("non_existing")) {
                assertThat("Non existing field\"" + fieldName + "\" should not be returned!", fieldsObject.terms(fieldName), nullValue());
                continue;
            }
            Terms terms = fieldsObject.terms(fieldName);
            assertThat("Existing field " + fieldName + "should have been returned", terms, notNullValue());
            // check overridden by keyword analyzer ...
            if (perFieldAnalyzer.containsKey(fieldName)) {
                TermsEnum iterator = terms.iterator();
                assertThat(
                    "Analyzer for " + fieldName + " should have been overridden!",
                    iterator.next().utf8ToString(),
                    equalTo("some text here")
                );
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

    public void testTermVectorsWithVersion() {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSettings(Settings.builder().put("index.refresh_interval", -1)));
        ensureGreen();

        TermVectorsResponse response = client().prepareTermVectors("test", "1").get();
        assertThat(response.isExists(), equalTo(false));

        logger.info("--> index doc 1");
        client().prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").get();

        // From translog:

        // version 0 means ignore version, which is the default
        response = client().prepareTermVectors(indexOrAlias(), "1").setVersion(Versions.MATCH_ANY).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(1L));

        response = client().prepareTermVectors(indexOrAlias(), "1").setVersion(1).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(1L));

        try {
            client().prepareGet(indexOrAlias(), "1").setVersion(2).get();
            fail();
        } catch (VersionConflictEngineException e) {
            // all good
        }

        // From Lucene index:
        refresh();

        // version 0 means ignore version, which is the default
        response = client().prepareTermVectors(indexOrAlias(), "1").setVersion(Versions.MATCH_ANY).setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(1L));

        response = client().prepareTermVectors(indexOrAlias(), "1").setVersion(1).setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(1L));

        try {
            client().prepareGet(indexOrAlias(), "1").setVersion(2).setRealtime(false).get();
            fail();
        } catch (VersionConflictEngineException e) {
            // all good
        }

        logger.info("--> index doc 1 again, so increasing the version");
        client().prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").get();

        // From translog:

        // version 0 means ignore version, which is the default
        response = client().prepareTermVectors(indexOrAlias(), "1").setVersion(Versions.MATCH_ANY).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2L));

        try {
            client().prepareGet(indexOrAlias(), "1").setVersion(1).get();
            fail();
        } catch (VersionConflictEngineException e) {
            // all good
        }

        response = client().prepareTermVectors(indexOrAlias(), "1").setVersion(2).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2L));

        // From Lucene index:
        refresh();

        // version 0 means ignore version, which is the default
        response = client().prepareTermVectors(indexOrAlias(), "1").setVersion(Versions.MATCH_ANY).setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2L));

        try {
            client().prepareGet(indexOrAlias(), "1").setVersion(1).setRealtime(false).get();
            fail();
        } catch (VersionConflictEngineException e) {
            // all good
        }

        response = client().prepareTermVectors(indexOrAlias(), "1").setVersion(2).setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2L));
    }

    public void testFilterLength() throws ExecutionException, InterruptedException, IOException {
        logger.info("Setting up the index ...");
        Settings.Builder settings = Settings.builder().put(indexSettings()).put("index.analysis.analyzer", "keyword");
        assertAcked(prepareCreate("test").setSettings(settings).setMapping("tags", "type=text"));

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
        indexRandom(true, client().prepareIndex("test").setId("1").setSource("tags", tags));

        logger.info("Checking best tags by longest to shortest size ...");
        TermVectorsRequest.FilterSettings filterSettings = new TermVectorsRequest.FilterSettings();
        filterSettings.maxNumTerms = numTerms;
        TermVectorsResponse response;
        for (int i = 0; i < numTerms; i++) {
            filterSettings.minWordLength = numTerms - i;
            response = client().prepareTermVectors("test", "1")
                .setSelectedFields("tags")
                .setFieldStatistics(true)
                .setTermStatistics(true)
                .setFilterSettings(filterSettings)
                .get();
            checkBestTerms(response.getFields().terms("tags"), tags.subList((numTerms - i - 1), numTerms));
        }
    }

    public void testFilterTermFreq() throws ExecutionException, InterruptedException, IOException {
        logger.info("Setting up the index ...");
        Settings.Builder settings = Settings.builder().put(indexSettings()).put("index.analysis.analyzer", "keyword");
        assertAcked(prepareCreate("test").setSettings(settings).setMapping("tags", "type=text"));

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
        indexRandom(true, client().prepareIndex("test").setId("1").setSource("tags", tags));

        logger.info("Checking best tags by highest to lowest term freq ...");
        TermVectorsRequest.FilterSettings filterSettings = new TermVectorsRequest.FilterSettings();
        TermVectorsResponse response;
        for (int i = 0; i < numTerms; i++) {
            filterSettings.maxNumTerms = i + 1;
            response = client().prepareTermVectors("test", "1")
                .setSelectedFields("tags")
                .setFieldStatistics(true)
                .setTermStatistics(true)
                .setFilterSettings(filterSettings)
                .get();
            checkBestTerms(response.getFields().terms("tags"), uniqueTags.subList((numTerms - i - 1), numTerms));
        }
    }

    public void testFilterDocFreq() throws ExecutionException, InterruptedException, IOException {
        logger.info("Setting up the index ...");
        Settings.Builder settings = Settings.builder()
            .put(indexSettings())
            .put("index.analysis.analyzer", "keyword")
            .put("index.number_of_shards", 1); // no dfs
        assertAcked(prepareCreate("test").setSettings(settings).setMapping("tags", "type=text"));

        int numDocs = scaledRandomIntBetween(10, 50); // as many terms as there are docs
        logger.info("Indexing {} documents with tags of increasing dfs ...", numDocs);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        List<String> tags = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            tags.add("tag_" + i);
            builders.add(client().prepareIndex("test").setId(i + "").setSource("tags", tags));
        }
        indexRandom(true, builders);

        logger.info("Checking best terms by highest to lowest idf ...");
        TermVectorsRequest.FilterSettings filterSettings = new TermVectorsRequest.FilterSettings();
        TermVectorsResponse response;
        for (int i = 0; i < numDocs; i++) {
            filterSettings.maxNumTerms = i + 1;
            response = client().prepareTermVectors("test", (numDocs - 1) + "")
                .setSelectedFields("tags")
                .setFieldStatistics(true)
                .setTermStatistics(true)
                .setFilterSettings(filterSettings)
                .get();
            checkBestTerms(response.getFields().terms("tags"), tags.subList((numDocs - i - 1), numDocs));
        }
    }

    public void testArtificialDocWithPreference() throws InterruptedException, IOException {
        // setup indices
        Settings.Builder settings = Settings.builder().put(indexSettings()).put("index.analysis.analyzer", "standard");
        assertAcked(prepareCreate("test").setSettings(settings).setMapping("field1", "type=text,term_vector=with_positions_offsets"));
        ensureGreen();

        // index document
        indexRandom(true, client().prepareIndex("test").setId("1").setSource("field1", "random permutation"));

        // Get search shards
        ClusterSearchShardsResponse searchShardsResponse = client().admin().cluster().prepareSearchShards("test").get();
        List<Integer> shardIds = Arrays.stream(searchShardsResponse.getGroups()).map(s -> s.getShardId().id()).toList();

        // request termvectors of artificial document from each shard
        int sumTotalTermFreq = 0;
        int sumDocFreq = 0;
        for (Integer shardId : shardIds) {
            TermVectorsResponse tvResponse = client().prepareTermVectors()
                .setIndex("test")
                .setPreference("_shards:" + shardId)
                .setDoc(jsonBuilder().startObject().field("field1", "random permutation").endObject())
                .setFieldStatistics(true)
                .setTermStatistics(true)
                .get();
            Fields fields = tvResponse.getFields();
            Terms terms = fields.terms("field1");
            assertNotNull(terms);
            TermsEnum termsEnum = terms.iterator();
            while (termsEnum.next() != null) {
                sumTotalTermFreq += termsEnum.totalTermFreq();
                sumDocFreq += termsEnum.docFreq();
            }
        }
        assertEquals("expected to find term statistics in exactly one shard!", 2, sumTotalTermFreq);
        assertEquals("expected to find term statistics in exactly one shard!", 2, sumDocFreq);
    }

    public void testWithKeywordAndNormalizer() throws IOException, ExecutionException, InterruptedException {
        // setup indices
        String[] indexNames = new String[] { "with_tv", "without_tv" };
        Settings.Builder builder = Settings.builder()
            .put(indexSettings())
            .put("index.analysis.analyzer.my_analyzer.tokenizer", "keyword")
            .putList("index.analysis.analyzer.my_analyzer.filter", "lowercase")
            .putList("index.analysis.normalizer.my_normalizer.filter", "lowercase");
        assertAcked(
            prepareCreate(indexNames[0]).setSettings(builder.build())
                .setMapping(
                    "field1",
                    "type=text,term_vector=with_positions_offsets,analyzer=my_analyzer",
                    "field2",
                    "type=text,term_vector=with_positions_offsets,analyzer=keyword"
                )
        );
        assertAcked(
            prepareCreate(indexNames[1]).setSettings(builder.build())
                .setMapping("field1", "type=keyword,normalizer=my_normalizer", "field2", "type=keyword")
        );
        ensureGreen();

        // index documents with and without term vectors
        String[] content = new String[] { "Hello World", "hello world", "HELLO WORLD" };

        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        for (String indexName : indexNames) {
            for (int id = 0; id < content.length; id++) {
                indexBuilders.add(
                    client().prepareIndex()
                        .setIndex(indexName)
                        .setId(String.valueOf(id))
                        .setSource("field1", content[id], "field2", content[id])
                );
            }
        }
        indexRandom(true, indexBuilders);

        // request tvs and compare from each index
        for (int id = 0; id < content.length; id++) {
            Fields[] fields = new Fields[2];
            for (int j = 0; j < indexNames.length; j++) {
                TermVectorsResponse resp = client().prepareTermVectors(indexNames[j], String.valueOf(id))
                    .setOffsets(true)
                    .setPositions(true)
                    .setSelectedFields("field1", "field2")
                    .get();
                assertThat("doc with index: " + indexNames[j] + ", type1 and id: " + id, resp.isExists(), equalTo(true));
                fields[j] = resp.getFields();
            }
            compareTermVectors("field1", fields[0], fields[1]);
            compareTermVectors("field2", fields[0], fields[1]);
        }
    }

    private void checkBestTerms(Terms terms, List<String> expectedTerms) throws IOException {
        final TermsEnum termsEnum = terms.iterator();
        List<String> bestTerms = new ArrayList<>();
        BytesRef text;
        while ((text = termsEnum.next()) != null) {
            bestTerms.add(text.utf8ToString());
        }
        Collections.sort(expectedTerms);
        Collections.sort(bestTerms);
        assertArrayEquals(expectedTerms.toArray(), bestTerms.toArray());
    }
}
