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

package org.elasticsearch.script;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class IndexLookupIT extends ESIntegTestCase {

    String includeAllFlag = "_FREQUENCIES | _OFFSETS | _PAYLOADS | _POSITIONS | _CACHE";
    String includeAllWithoutRecordFlag = "_FREQUENCIES | _OFFSETS | _PAYLOADS | _POSITIONS ";
    private HashMap<String, List<Object>> expectedEndOffsetsArray;
    private HashMap<String, List<Object>> expectedPayloadsArray;
    private HashMap<String, List<Object>> expectedPositionsArray;
    private HashMap<String, List<Object>> emptyArray;
    private HashMap<String, List<Object>> expectedStartOffsetsArray;

    void initTestData() throws InterruptedException, ExecutionException, IOException {
        emptyArray = new HashMap<>();
        List<Object> empty1 = new ArrayList<>();
        empty1.add(-1);
        empty1.add(-1);
        emptyArray.put("1", empty1);
        List<Object> empty2 = new ArrayList<>();
        empty2.add(-1);
        empty2.add(-1);
        emptyArray.put("2", empty2);
        List<Object> empty3 = new ArrayList<>();
        empty3.add(-1);
        empty3.add(-1);
        emptyArray.put("3", empty3);

        expectedPositionsArray = new HashMap<>();

        List<Object> pos1 = new ArrayList<>();
        pos1.add(1);
        pos1.add(2);
        expectedPositionsArray.put("1", pos1);
        List<Object> pos2 = new ArrayList<>();
        pos2.add(0);
        pos2.add(1);
        expectedPositionsArray.put("2", pos2);
        List<Object> pos3 = new ArrayList<>();
        pos3.add(0);
        pos3.add(4);
        expectedPositionsArray.put("3", pos3);

        expectedPayloadsArray = new HashMap<>();
        List<Object> pay1 = new ArrayList<>();
        pay1.add(2);
        pay1.add(3);
        expectedPayloadsArray.put("1", pay1);
        List<Object> pay2 = new ArrayList<>();
        pay2.add(1);
        pay2.add(2);
        expectedPayloadsArray.put("2", pay2);
        List<Object> pay3 = new ArrayList<>();
        pay3.add(1);
        pay3.add(-1);
        expectedPayloadsArray.put("3", pay3);
        /*
         * "a|1 b|2 b|3 c|4 d " "b|1 b|2 c|3 d|4 a " "b|1 c|2 d|3 a|4 b "
         */
        expectedStartOffsetsArray = new HashMap<>();
        List<Object> starts1 = new ArrayList<>();
        starts1.add(4);
        starts1.add(8);
        expectedStartOffsetsArray.put("1", starts1);
        List<Object> starts2 = new ArrayList<>();
        starts2.add(0);
        starts2.add(4);
        expectedStartOffsetsArray.put("2", starts2);
        List<Object> starts3 = new ArrayList<>();
        starts3.add(0);
        starts3.add(16);
        expectedStartOffsetsArray.put("3", starts3);

        expectedEndOffsetsArray = new HashMap<>();
        List<Object> ends1 = new ArrayList<>();
        ends1.add(7);
        ends1.add(11);
        expectedEndOffsetsArray.put("1", ends1);
        List<Object> ends2 = new ArrayList<>();
        ends2.add(3);
        ends2.add(7);
        expectedEndOffsetsArray.put("2", ends2);
        List<Object> ends3 = new ArrayList<>();
        ends3.add(3);
        ends3.add(17);
        expectedEndOffsetsArray.put("3", ends3);

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("int_payload_field").field("type", "string").field("index_options", "offsets")
                .field("analyzer", "payload_int").endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", mapping).setSettings(
                Settings.settingsBuilder()
                        .put(indexSettings())
                        .put("index.analysis.analyzer.payload_int.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.payload_int.filter", "delimited_int")
                        .put("index.analysis.filter.delimited_int.delimiter", "|")
                        .put("index.analysis.filter.delimited_int.encoding", "int")
                        .put("index.analysis.filter.delimited_int.type", "delimited_payload_filter")));
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("int_payload_field", "a|1 b|2 b|3 c|4 d "), client()
                .prepareIndex("test", "type1", "2").setSource("int_payload_field", "b|1 b|2 c|3 d|4 a "),
                client().prepareIndex("test", "type1", "3").setSource("int_payload_field", "b|1 c|2 d|3 a|4 b "));
        ensureGreen();
    }

    @Test
    public void testTwoScripts() throws Exception {

        initTestData();

        // check term frequencies for 'a'
        Script scriptFieldScript = new Script("term = _index['int_payload_field']['c']; term.tf()");
        scriptFieldScript = new Script("1");
        Script scoreScript = new Script("term = _index['int_payload_field']['b']; term.tf()");
        Map<String, Object> expectedResultsField = new HashMap<>();
        expectedResultsField.put("1", 1);
        expectedResultsField.put("2", 1);
        expectedResultsField.put("3", 1);
        Map<String, Object> expectedResultsScore = new HashMap<>();
        expectedResultsScore.put("1", 2f);
        expectedResultsScore.put("2", 2f);
        expectedResultsScore.put("3", 2f);
        checkOnlyFunctionScore(scoreScript, expectedResultsScore, 3);
        checkValueInEachDocWithFunctionScore(scriptFieldScript, expectedResultsField, scoreScript, expectedResultsScore, 3);

    }

    @Test
    public void testCallWithDifferentFlagsFails() throws Exception {

        initTestData();

        // should throw an exception, we cannot call with different flags twice
        // if the flags of the second call were not included in the first call.
        Script script = new Script("term = _index['int_payload_field']['b']; return _index['int_payload_field'].get('b', _POSITIONS).tf();");
        try {
            client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).addScriptField("tvtest", script).execute().actionGet();
        } catch (SearchPhaseExecutionException e) {
            assertThat(
                    "got: " + e.toString(),
                    e.toString()
                            .indexOf(
                                    "You must call get with all required flags! Instead of  _index['int_payload_field'].get('b', _FREQUENCIES) and _index['int_payload_field'].get('b', _POSITIONS) call  _index['int_payload_field'].get('b', _FREQUENCIES | _POSITIONS)  once]"),
                    Matchers.greaterThan(-1));
        }

        // Should not throw an exception this way round
        script = new Script(
                "term = _index['int_payload_field'].get('b', _POSITIONS | _FREQUENCIES);return _index['int_payload_field']['b'].tf();");
        client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).addScriptField("tvtest", script).execute().actionGet();
    }

    private void checkOnlyFunctionScore(Script scoreScript, Map<String, Object> expectedScore, int numExpectedDocs) {
        SearchResponse sr = client().prepareSearch("test")
                .setQuery(QueryBuilders.functionScoreQuery(ScoreFunctionBuilders.scriptFunction(scoreScript))).execute()
                .actionGet();
        assertHitCount(sr, numExpectedDocs);
        for (SearchHit hit : sr.getHits().getHits()) {
            assertThat("for doc " + hit.getId(), ((Float) expectedScore.get(hit.getId())).doubleValue(),
                    Matchers.closeTo(hit.score(), 1.e-4));
        }
    }

    @Test
    public void testDocumentationExample() throws Exception {

        initTestData();

        Script script = new Script("term = _index['float_payload_field'].get('b'," + includeAllFlag
                + "); payloadSum=0; for (pos in term) {payloadSum = pos.payloadAsInt(0)}; payloadSum");

        // non existing field: sum should be 0
        HashMap<String, Object> zeroArray = new HashMap<>();
        zeroArray.put("1", 0);
        zeroArray.put("2", 0);
        zeroArray.put("3", 0);
        checkValueInEachDoc(script, zeroArray, 3);

        script = new Script("term = _index['int_payload_field'].get('b'," + includeAllFlag
                + "); payloadSum=0; for (pos in term) {payloadSum = payloadSum + pos.payloadAsInt(0)}; payloadSum");

        // existing field: sums should be as here:
        zeroArray.put("1", 5);
        zeroArray.put("2", 3);
        zeroArray.put("3", 1);
        checkValueInEachDoc(script, zeroArray, 3);
    }

    @Test
    public void testIteratorAndRecording() throws Exception {

        initTestData();

        // call twice with record: should work as expected
        Script script = createPositionsArrayScriptIterateTwice("b", includeAllFlag, "position");
        checkArrayValsInEachDoc(script, expectedPositionsArray, 3);
        script = createPositionsArrayScriptIterateTwice("b", includeAllFlag, "startOffset");
        checkArrayValsInEachDoc(script, expectedStartOffsetsArray, 3);
        script = createPositionsArrayScriptIterateTwice("b", includeAllFlag, "endOffset");
        checkArrayValsInEachDoc(script, expectedEndOffsetsArray, 3);
        script = createPositionsArrayScriptIterateTwice("b", includeAllFlag, "payloadAsInt(-1)");
        checkArrayValsInEachDoc(script, expectedPayloadsArray, 3);

        // no record and get iterator twice: should fail
        script = createPositionsArrayScriptIterateTwice("b", includeAllWithoutRecordFlag, "position");
        checkExceptions(script);
        script = createPositionsArrayScriptIterateTwice("b", includeAllWithoutRecordFlag, "startOffset");
        checkExceptions(script);
        script = createPositionsArrayScriptIterateTwice("b", includeAllWithoutRecordFlag, "endOffset");
        checkExceptions(script);
        script = createPositionsArrayScriptIterateTwice("b", includeAllWithoutRecordFlag, "payloadAsInt(-1)");
        checkExceptions(script);

        // no record and get termObject twice and iterate: should fail
        script = createPositionsArrayScriptGetInfoObjectTwice("b", includeAllWithoutRecordFlag, "position");
        checkExceptions(script);
        script = createPositionsArrayScriptGetInfoObjectTwice("b", includeAllWithoutRecordFlag, "startOffset");
        checkExceptions(script);
        script = createPositionsArrayScriptGetInfoObjectTwice("b", includeAllWithoutRecordFlag, "endOffset");
        checkExceptions(script);
        script = createPositionsArrayScriptGetInfoObjectTwice("b", includeAllWithoutRecordFlag, "payloadAsInt(-1)");
        checkExceptions(script);

    }

    private Script createPositionsArrayScriptGetInfoObjectTwice(String term, String flags, String what) {
        String script = "term = _index['int_payload_field'].get('" + term + "'," + flags
                + "); array=[]; for (pos in term) {array.add(pos." + what + ")}; _index['int_payload_field'].get('" + term + "',"
                + flags + "); array=[]; for (pos in term) {array.add(pos." + what + ")}";
        return new Script(script);
    }

    private Script createPositionsArrayScriptIterateTwice(String term, String flags, String what) {
        String script = "term = _index['int_payload_field'].get('" + term + "'," + flags
                + "); array=[]; for (pos in term) {array.add(pos." + what + ")}; array=[]; for (pos in term) {array.add(pos." + what
                + ")}; array";
        return new Script(script);
    }

    private Script createPositionsArrayScript(String field, String term, String flags, String what) {
        String script = "term = _index['" + field + "'].get('" + term + "'," + flags
                + "); array=[]; for (pos in term) {array.add(pos." + what + ")}; array";
        return new Script(script);
    }

    private Script createPositionsArrayScriptDefaultGet(String field, String term, String what) {
        String script = "term = _index['" + field + "']['" + term + "']; array=[]; for (pos in term) {array.add(pos." + what
                + ")}; array";
        return new Script(script);
    }

    @Test
    public void testFlags() throws Exception {

        initTestData();

        // check default flag
        Script script = createPositionsArrayScriptDefaultGet("int_payload_field", "b", "position");
        // there should be no positions
        /* TODO: the following tests fail with the new postings enum apis because of a bogus assert in BlockDocsEnum
        checkArrayValsInEachDoc(script, emptyArray, 3);
        script = createPositionsArrayScriptDefaultGet("int_payload_field", "b", "startOffset");
        // there should be no offsets
        checkArrayValsInEachDoc(script, emptyArray, 3);
        script = createPositionsArrayScriptDefaultGet("int_payload_field", "b", "endOffset");
        // there should be no offsets
        checkArrayValsInEachDoc(script, emptyArray, 3);
        script = createPositionsArrayScriptDefaultGet("int_payload_field", "b", "payloadAsInt(-1)");
        // there should be no payload
        checkArrayValsInEachDoc(script, emptyArray, 3);

        // check FLAG_FREQUENCIES flag
        script = createPositionsArrayScript("int_payload_field", "b", "_FREQUENCIES", "position");
        // there should be no positions
        checkArrayValsInEachDoc(script, emptyArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", "_FREQUENCIES", "startOffset");
        // there should be no offsets
        checkArrayValsInEachDoc(script, emptyArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", "_FREQUENCIES", "endOffset");
        // there should be no offsets
        checkArrayValsInEachDoc(script, emptyArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", "_FREQUENCIES", "payloadAsInt(-1)");
        // there should be no payloads
        checkArrayValsInEachDoc(script, emptyArray, 3);*/

        // check FLAG_POSITIONS flag
        script = createPositionsArrayScript("int_payload_field", "b", "_POSITIONS", "position");
        // there should be positions
        checkArrayValsInEachDoc(script, expectedPositionsArray, 3);
        /* TODO: these tests make a bogus assumption that asking for positions will return only positions
        script = createPositionsArrayScript("int_payload_field", "b", "_POSITIONS", "startOffset");
        // there should be no offsets
        checkArrayValsInEachDoc(script, emptyArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", "_POSITIONS", "endOffset");
        // there should be no offsets
        checkArrayValsInEachDoc(script, emptyArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", "_POSITIONS", "payloadAsInt(-1)");
        // there should be no payloads
        checkArrayValsInEachDoc(script, emptyArray, 3);*/

        // check FLAG_OFFSETS flag
        script = createPositionsArrayScript("int_payload_field", "b", "_OFFSETS", "position");
        // there should be positions and s forth ...
        checkArrayValsInEachDoc(script, expectedPositionsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", "_OFFSETS", "startOffset");
        checkArrayValsInEachDoc(script, expectedStartOffsetsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", "_OFFSETS", "endOffset");
        checkArrayValsInEachDoc(script, expectedEndOffsetsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", "_OFFSETS", "payloadAsInt(-1)");
        checkArrayValsInEachDoc(script, expectedPayloadsArray, 3);

        // check FLAG_PAYLOADS flag
        script = createPositionsArrayScript("int_payload_field", "b", "_PAYLOADS", "position");
        checkArrayValsInEachDoc(script, expectedPositionsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", "_PAYLOADS", "startOffset");
        checkArrayValsInEachDoc(script, expectedStartOffsetsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", "_PAYLOADS", "endOffset");
        checkArrayValsInEachDoc(script, expectedEndOffsetsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", "_PAYLOADS", "payloadAsInt(-1)");
        checkArrayValsInEachDoc(script, expectedPayloadsArray, 3);

        // check all flags
        String allFlags = "_POSITIONS | _OFFSETS | _PAYLOADS";
        script = createPositionsArrayScript("int_payload_field", "b", allFlags, "position");
        checkArrayValsInEachDoc(script, expectedPositionsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", allFlags, "startOffset");
        checkArrayValsInEachDoc(script, expectedStartOffsetsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", allFlags, "endOffset");
        checkArrayValsInEachDoc(script, expectedEndOffsetsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", allFlags, "payloadAsInt(-1)");
        checkArrayValsInEachDoc(script, expectedPayloadsArray, 3);

        // check all flags without record
        script = createPositionsArrayScript("int_payload_field", "b", includeAllWithoutRecordFlag, "position");
        checkArrayValsInEachDoc(script, expectedPositionsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", includeAllWithoutRecordFlag, "startOffset");
        checkArrayValsInEachDoc(script, expectedStartOffsetsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", includeAllWithoutRecordFlag, "endOffset");
        checkArrayValsInEachDoc(script, expectedEndOffsetsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", includeAllWithoutRecordFlag, "payloadAsInt(-1)");
        checkArrayValsInEachDoc(script, expectedPayloadsArray, 3);

    }

    private void checkArrayValsInEachDoc(Script script, HashMap<String, List<Object>> expectedArray, int expectedHitSize) {
        SearchResponse sr = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).addScriptField("tvtest", script)
                .execute().actionGet();
        assertHitCount(sr, expectedHitSize);
        int nullCounter = 0;
        for (SearchHit hit : sr.getHits().getHits()) {
            Object result = hit.getFields().get("tvtest").getValues();
            Object expectedResult = expectedArray.get(hit.getId());
            assertThat("for doc " + hit.getId(), result, equalTo(expectedResult));
            if (expectedResult != null) {
                nullCounter++;
            }
        }
        assertThat(nullCounter, equalTo(expectedArray.size()));
    }

    @Test
    public void testAllExceptPosAndOffset() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("float_payload_field").field("type", "string").field("index_options", "offsets").field("term_vector", "no")
            .field("analyzer", "payload_float").endObject().startObject("string_payload_field").field("type", "string")
            .field("index_options", "offsets").field("term_vector", "no").field("analyzer", "payload_string").endObject()
                .startObject("int_payload_field").field("type", "string").field("index_options", "offsets")
            .field("analyzer", "payload_int").endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", mapping).setSettings(
                Settings.settingsBuilder()
                        .put(indexSettings())
                        .put("index.analysis.analyzer.payload_float.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.payload_float.filter", "delimited_float")
                        .put("index.analysis.filter.delimited_float.delimiter", "|")
                        .put("index.analysis.filter.delimited_float.encoding", "float")
                        .put("index.analysis.filter.delimited_float.type", "delimited_payload_filter")
                        .put("index.analysis.analyzer.payload_string.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.payload_string.filter", "delimited_string")
                        .put("index.analysis.filter.delimited_string.delimiter", "|")
                        .put("index.analysis.filter.delimited_string.encoding", "identity")
                        .put("index.analysis.filter.delimited_string.type", "delimited_payload_filter")
                        .put("index.analysis.analyzer.payload_int.tokenizer", "whitespace")
                        .putArray("index.analysis.analyzer.payload_int.filter", "delimited_int")
                        .put("index.analysis.filter.delimited_int.delimiter", "|")
                        .put("index.analysis.filter.delimited_int.encoding", "int")
                        .put("index.analysis.filter.delimited_int.type", "delimited_payload_filter")
                        .put("index.number_of_shards", 1)));
        ensureYellow();
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("float_payload_field", "a|1 b|2 a|3 b "), client()
                .prepareIndex("test", "type1", "2").setSource("string_payload_field", "a|a b|b a|a b "),
                client().prepareIndex("test", "type1", "3").setSource("float_payload_field", "a|4 b|5 a|6 b "),
                client().prepareIndex("test", "type1", "4").setSource("string_payload_field", "a|b b|a a|b b "),
                client().prepareIndex("test", "type1", "5").setSource("float_payload_field", "c "),
                client().prepareIndex("test", "type1", "6").setSource("int_payload_field", "c|1"));

        // get the number of all docs
        Script script = new Script("_index.numDocs()");
        checkValueInEachDoc(6, script, 6);

        // get the number of docs with field float_payload_field
        script = new Script("_index['float_payload_field'].docCount()");
        checkValueInEachDoc(3, script, 6);

        // corner case: what if the field does not exist?
        script = new Script("_index['non_existent_field'].docCount()");
        checkValueInEachDoc(0, script, 6);

        // get the number of all tokens in all docs
        script = new Script("_index['float_payload_field'].sumttf()");
        checkValueInEachDoc(9, script, 6);

        // corner case get the number of all tokens in all docs for non existent
        // field
        script = new Script("_index['non_existent_field'].sumttf()");
        checkValueInEachDoc(0, script, 6);

        // get the sum of doc freqs in all docs
        script = new Script("_index['float_payload_field'].sumdf()");
        checkValueInEachDoc(5, script, 6);

        // get the sum of doc freqs in all docs for non existent field
        script = new Script("_index['non_existent_field'].sumdf()");
        checkValueInEachDoc(0, script, 6);

        // check term frequencies for 'a'
        script = new Script("term = _index['float_payload_field']['a']; if (term != null) {term.tf()}");
        Map<String, Object> expectedResults = new HashMap<>();
        expectedResults.put("1", 2);
        expectedResults.put("2", 0);
        expectedResults.put("3", 2);
        expectedResults.put("4", 0);
        expectedResults.put("5", 0);
        expectedResults.put("6", 0);
        checkValueInEachDoc(script, expectedResults, 6);
        expectedResults.clear();

        // check doc frequencies for 'c'
        script = new Script("term = _index['float_payload_field']['c']; if (term != null) {term.df()}");
        expectedResults.put("1", 1l);
        expectedResults.put("2", 1l);
        expectedResults.put("3", 1l);
        expectedResults.put("4", 1l);
        expectedResults.put("5", 1l);
        expectedResults.put("6", 1l);
        checkValueInEachDoc(script, expectedResults, 6);
        expectedResults.clear();

        // check doc frequencies for term that does not exist
        script = new Script("term = _index['float_payload_field']['non_existent_term']; if (term != null) {term.df()}");
        expectedResults.put("1", 0l);
        expectedResults.put("2", 0l);
        expectedResults.put("3", 0l);
        expectedResults.put("4", 0l);
        expectedResults.put("5", 0l);
        expectedResults.put("6", 0l);
        checkValueInEachDoc(script, expectedResults, 6);
        expectedResults.clear();

        // check doc frequencies for term that does not exist
        script = new Script("term = _index['non_existent_field']['non_existent_term']; if (term != null) {term.tf()}");
        expectedResults.put("1", 0);
        expectedResults.put("2", 0);
        expectedResults.put("3", 0);
        expectedResults.put("4", 0);
        expectedResults.put("5", 0);
        expectedResults.put("6", 0);
        checkValueInEachDoc(script, expectedResults, 6);
        expectedResults.clear();

        // check total term frequencies for 'a'
        script = new Script("term = _index['float_payload_field']['a']; if (term != null) {term.ttf()}");
        expectedResults.put("1", 4l);
        expectedResults.put("2", 4l);
        expectedResults.put("3", 4l);
        expectedResults.put("4", 4l);
        expectedResults.put("5", 4l);
        expectedResults.put("6", 4l);
        checkValueInEachDoc(script, expectedResults, 6);
        expectedResults.clear();

        // check float payload for 'b'
        HashMap<String, List<Object>> expectedPayloadsArray = new HashMap<>();
        script = createPositionsArrayScript("float_payload_field", "b", includeAllFlag, "payloadAsFloat(-1)");
        float missingValue = -1;
        List<Object> payloadsFor1 = new ArrayList<>();
        payloadsFor1.add(2f);
        payloadsFor1.add(missingValue);
        expectedPayloadsArray.put("1", payloadsFor1);
        List<Object> payloadsFor2 = new ArrayList<>();
        payloadsFor2.add(5f);
        payloadsFor2.add(missingValue);
        expectedPayloadsArray.put("3", payloadsFor2);
        expectedPayloadsArray.put("6", new ArrayList<>());
        expectedPayloadsArray.put("5", new ArrayList<>());
        expectedPayloadsArray.put("4", new ArrayList<>());
        expectedPayloadsArray.put("2", new ArrayList<>());
        checkArrayValsInEachDoc(script, expectedPayloadsArray, 6);

        // check string payload for 'b'
        expectedPayloadsArray.clear();
        payloadsFor1.clear();
        payloadsFor2.clear();
        script = createPositionsArrayScript("string_payload_field", "b", includeAllFlag, "payloadAsString()");
        payloadsFor1.add("b");
        payloadsFor1.add(null);
        expectedPayloadsArray.put("2", payloadsFor1);
        payloadsFor2.add("a");
        payloadsFor2.add(null);
        expectedPayloadsArray.put("4", payloadsFor2);
        expectedPayloadsArray.put("6", new ArrayList<>());
        expectedPayloadsArray.put("5", new ArrayList<>());
        expectedPayloadsArray.put("3", new ArrayList<>());
        expectedPayloadsArray.put("1", new ArrayList<>());
        checkArrayValsInEachDoc(script, expectedPayloadsArray, 6);

        // check int payload for 'c'
        expectedPayloadsArray.clear();
        payloadsFor1.clear();
        payloadsFor2.clear();
        script = createPositionsArrayScript("int_payload_field", "c", includeAllFlag, "payloadAsInt(-1)");
        payloadsFor1 = new ArrayList<>();
        payloadsFor1.add(1);
        expectedPayloadsArray.put("6", payloadsFor1);
        expectedPayloadsArray.put("5", new ArrayList<>());
        expectedPayloadsArray.put("4", new ArrayList<>());
        expectedPayloadsArray.put("3", new ArrayList<>());
        expectedPayloadsArray.put("2", new ArrayList<>());
        expectedPayloadsArray.put("1", new ArrayList<>());
        checkArrayValsInEachDoc(script, expectedPayloadsArray, 6);

    }

    private void checkExceptions(Script script) {
        try {
            SearchResponse sr = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).addScriptField("tvtest", script)
                    .execute().actionGet();
            assertThat(sr.getHits().hits().length, equalTo(0));
            ShardSearchFailure[] shardFails = sr.getShardFailures();
            for (ShardSearchFailure fail : shardFails) {
                assertThat(fail.reason().indexOf("Cannot iterate twice! If you want to iterate more that once, add _CACHE explicitly."),
                        Matchers.greaterThan(-1));
            }
        } catch (SearchPhaseExecutionException ex) {
            assertThat(
                    "got " + ex.toString(),
                    ex.toString().indexOf("Cannot iterate twice! If you want to iterate more that once, add _CACHE explicitly."),
                    Matchers.greaterThan(-1));
        }
    }

    private void checkValueInEachDocWithFunctionScore(Script fieldScript, Map<String, Object> expectedFieldVals, Script scoreScript,
            Map<String, Object> expectedScore, int numExpectedDocs) {
        SearchResponse sr = client().prepareSearch("test")
                .setQuery(QueryBuilders.functionScoreQuery(ScoreFunctionBuilders.scriptFunction(scoreScript)))
                .addScriptField("tvtest", fieldScript).execute().actionGet();
        assertHitCount(sr, numExpectedDocs);
        for (SearchHit hit : sr.getHits().getHits()) {
            Object result = hit.getFields().get("tvtest").getValues().get(0);
            Object expectedResult = expectedFieldVals.get(hit.getId());
            assertThat("for doc " + hit.getId(), result, equalTo(expectedResult));
            assertThat("for doc " + hit.getId(), ((Float) expectedScore.get(hit.getId())).doubleValue(),
                    Matchers.closeTo(hit.score(), 1.e-4));
        }
    }

    private void checkValueInEachDoc(Script script, Map<String, Object> expectedResults, int numExpectedDocs) {
        SearchResponse sr = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).addScriptField("tvtest", script)
                .execute().actionGet();
        assertHitCount(sr, numExpectedDocs);
        for (SearchHit hit : sr.getHits().getHits()) {
            Object result = hit.getFields().get("tvtest").getValues().get(0);
            Object expectedResult = expectedResults.get(hit.getId());
            assertThat("for doc " + hit.getId(), result, equalTo(expectedResult));
        }
    }

    private void checkValueInEachDoc(int value, Script script, int numExpectedDocs) {
        SearchResponse sr = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).addScriptField("tvtest", script)
                .execute().actionGet();
        assertHitCount(sr, numExpectedDocs);
        for (SearchHit hit : sr.getHits().getHits()) {
            Object result = hit.getFields().get("tvtest").getValues().get(0);
            if (result instanceof Integer) {
                assertThat((Integer)result, equalTo(value));
            } else if (result instanceof Long) {
                assertThat(((Long) result).intValue(), equalTo(value));
            } else {
                fail();
            }
        }
    }
}
