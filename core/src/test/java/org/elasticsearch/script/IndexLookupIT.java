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
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;
import org.elasticsearch.search.lookup.IndexLookup;
import org.elasticsearch.search.lookup.LeafIndexLookup;
import org.elasticsearch.search.lookup.TermPosition;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.script.ScriptService.ScriptType;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class IndexLookupIT extends ESIntegTestCase {

    private static final String INCLUDE_ALL = "_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS|_CACHE";
    private static final int ALL_FLAGS = IndexLookup.FLAG_FREQUENCIES
            | IndexLookup.FLAG_OFFSETS
            | IndexLookup.FLAG_PAYLOADS
            | IndexLookup.FLAG_POSITIONS
            | IndexLookup.FLAG_CACHE;

    private static final String INCLUDE_ALL_BUT_CACHE = "_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS";
    private static final int ALL_FLAGS_WITHOUT_CACHE = IndexLookup.FLAG_FREQUENCIES
            | IndexLookup.FLAG_OFFSETS
            | IndexLookup.FLAG_PAYLOADS
            | IndexLookup.FLAG_POSITIONS;

    private HashMap<String, List<Object>> expectedEndOffsetsArray;
    private HashMap<String, List<Object>> expectedPayloadsArray;
    private HashMap<String, List<Object>> expectedPositionsArray;
    private HashMap<String, List<Object>> expectedStartOffsetsArray;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("term = _index['int_payload_field']['c']; term.tf()", vars -> tf(vars, "int_payload_field", "c"));
            scripts.put("term = _index['int_payload_field']['b']; term.tf()", vars -> tf(vars, "int_payload_field", "b"));

            scripts.put("Sum the payloads of [float_payload_field][b]", vars -> payloadSum(vars, "float_payload_field", "b"));
            scripts.put("Sum the payloads of [int_payload_field][b]", vars -> payloadSum(vars, "int_payload_field", "b"));

            scripts.put("createPositionsArrayScriptIterateTwice[b," + INCLUDE_ALL + ",position]",
                    vars -> createPositionsArrayScriptIterateTwice(vars, "b", ALL_FLAGS, p -> p.position));
            scripts.put("createPositionsArrayScriptIterateTwice[b," + INCLUDE_ALL + ",startOffset]",
                    vars -> createPositionsArrayScriptIterateTwice(vars, "b", ALL_FLAGS, p -> p.startOffset));
            scripts.put("createPositionsArrayScriptIterateTwice[b," + INCLUDE_ALL + ",endOffset]",
                    vars -> createPositionsArrayScriptIterateTwice(vars, "b", ALL_FLAGS, p -> p.endOffset));
            scripts.put("createPositionsArrayScriptIterateTwice[b," + INCLUDE_ALL + ",payloadAsInt(-1)]",
                    vars -> createPositionsArrayScriptIterateTwice(vars, "b", ALL_FLAGS, p -> p.payloadAsInt(-1)));

            scripts.put("createPositionsArrayScriptIterateTwice[b,_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS,position]",
                    vars -> createPositionsArrayScriptIterateTwice(vars, "b", ALL_FLAGS_WITHOUT_CACHE, p -> p.position));
            scripts.put("createPositionsArrayScriptIterateTwice[b,_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS,startOffset]",
                    vars -> createPositionsArrayScriptIterateTwice(vars, "b", ALL_FLAGS_WITHOUT_CACHE, p -> p.startOffset));
            scripts.put("createPositionsArrayScriptIterateTwice[b,_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS,endOffset]",
                    vars -> createPositionsArrayScriptIterateTwice(vars, "b", ALL_FLAGS_WITHOUT_CACHE, p -> p.endOffset));
            scripts.put("createPositionsArrayScriptIterateTwice[b,_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS,payloadAsInt(-1)]",
                    vars -> createPositionsArrayScriptIterateTwice(vars, "b", ALL_FLAGS_WITHOUT_CACHE, p -> p.payloadAsInt(-1)));

            scripts.put("createPositionsArrayScriptGetInfoObjectTwice[b,_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS,position]",
                    vars -> createPositionsArrayScriptGetInfoObjectTwice(vars, "b", ALL_FLAGS_WITHOUT_CACHE, p -> p.position));
            scripts.put("createPositionsArrayScriptGetInfoObjectTwice[b,_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS,startOffset]",
                    vars -> createPositionsArrayScriptGetInfoObjectTwice(vars, "b", ALL_FLAGS_WITHOUT_CACHE, p -> p.startOffset));
            scripts.put("createPositionsArrayScriptGetInfoObjectTwice[b,_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS,endOffset]",
                    vars -> createPositionsArrayScriptGetInfoObjectTwice(vars, "b", ALL_FLAGS_WITHOUT_CACHE, p -> p.endOffset));
            scripts.put("createPositionsArrayScriptGetInfoObjectTwice[b,_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS,payloadAsInt(-1)]",
                    vars -> createPositionsArrayScriptGetInfoObjectTwice(vars, "b", ALL_FLAGS_WITHOUT_CACHE, p -> p.payloadAsInt(-1)));

            scripts.put("createPositionsArrayScript[int_payload_field,b,_POSITIONS,position]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", IndexLookup.FLAG_POSITIONS, p -> p.position));

            scripts.put("createPositionsArrayScript[int_payload_field,b,_OFFSETS,position]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", IndexLookup.FLAG_OFFSETS, p -> p.position));
            scripts.put("createPositionsArrayScript[int_payload_field,b,_OFFSETS,startOffset]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", IndexLookup.FLAG_OFFSETS, p -> p.startOffset));
            scripts.put("createPositionsArrayScript[int_payload_field,b,_OFFSETS,endOffset]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", IndexLookup.FLAG_OFFSETS, p -> p.endOffset));
            scripts.put("createPositionsArrayScript[int_payload_field,b,_OFFSETS,payloadAsInt(-1)]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", IndexLookup.FLAG_OFFSETS, p -> p.payloadAsInt(-1)));

            scripts.put("createPositionsArrayScript[int_payload_field,b,_PAYLOADS,position]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", IndexLookup.FLAG_PAYLOADS, p -> p.position));
            scripts.put("createPositionsArrayScript[int_payload_field,b,_PAYLOADS,startOffset]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", IndexLookup.FLAG_PAYLOADS, p -> p.startOffset));
            scripts.put("createPositionsArrayScript[int_payload_field,b,_PAYLOADS,endOffset]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", IndexLookup.FLAG_PAYLOADS, p -> p.endOffset));
            scripts.put("createPositionsArrayScript[int_payload_field,b,_PAYLOADS,payloadAsInt(-1)]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", IndexLookup.FLAG_PAYLOADS, p -> p.payloadAsInt(-1)));

            int posoffpay = IndexLookup.FLAG_POSITIONS|IndexLookup.FLAG_OFFSETS|IndexLookup.FLAG_PAYLOADS;
            scripts.put("createPositionsArrayScript[int_payload_field,b,_POSITIONS|_OFFSETS|_PAYLOADS,position]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", posoffpay, p -> p.position));
            scripts.put("createPositionsArrayScript[int_payload_field,b,_POSITIONS|_OFFSETS|_PAYLOADS,startOffset]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", posoffpay, p -> p.startOffset));
            scripts.put("createPositionsArrayScript[int_payload_field,b,_POSITIONS|_OFFSETS|_PAYLOADS,endOffset]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", posoffpay, p -> p.endOffset));
            scripts.put("createPositionsArrayScript[int_payload_field,b,_POSITIONS|_OFFSETS|_PAYLOADS,payloadAsInt(-1)]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", posoffpay, p -> p.payloadAsInt(-1)));

            scripts.put("createPositionsArrayScript[int_payload_field,b,_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS,position]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", ALL_FLAGS_WITHOUT_CACHE, p -> p.position));
            scripts.put("createPositionsArrayScript[int_payload_field,b,_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS,startOffset]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", ALL_FLAGS_WITHOUT_CACHE, p -> p.startOffset));
            scripts.put("createPositionsArrayScript[int_payload_field,b,_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS,endOffset]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", ALL_FLAGS_WITHOUT_CACHE, p -> p.endOffset));
            scripts.put("createPositionsArrayScript[int_payload_field,b,_FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS,payloadAsInt(-1)]",
                    vars -> createPositionsArrayScript(vars, "int_payload_field", "b", ALL_FLAGS_WITHOUT_CACHE, p -> p.payloadAsInt(-1)));

            scripts.put("createPositionsArrayScript" +
                            "[float_payload_field,b," + INCLUDE_ALL + ",payloadAsFloat(-1)]",
                    vars -> createPositionsArrayScript(vars,"float_payload_field", "b", ALL_FLAGS, p -> p.payloadAsFloat(-1)));
            scripts.put("createPositionsArrayScript" +
                            "[string_payload_field,b," + INCLUDE_ALL + ",payloadAsString()]",
                    vars -> createPositionsArrayScript(vars,"string_payload_field", "b", ALL_FLAGS, TermPosition::payloadAsString));
            scripts.put("createPositionsArrayScript" +
                            "[int_payload_field,c," + INCLUDE_ALL + ",payloadAsInt(-1)]",
                    vars -> createPositionsArrayScript(vars,"int_payload_field", "c", ALL_FLAGS, p -> p.payloadAsInt(-1)));

            // Call with different flags twice, equivalent to:
            //      term = _index['int_payload_field']['b']; return _index['int_payload_field'].get('b', _POSITIONS).tf();
            scripts.put("Call with different flags twice", vars -> {
                LeafIndexLookup leafIndexLookup = (LeafIndexLookup) vars.get("_index");
                IndexField indexField = leafIndexLookup.get("int_payload_field");

                // 1st call
                indexField.get("b");
                try {
                    // 2nd call, must throws an exception
                    return indexField.get("b", IndexLookup.FLAG_POSITIONS).tf();
                } catch (IOException e) {
                    throw new ScriptException(e.getMessage(), e, emptyList(), "Call with different flags twice", CustomScriptPlugin.NAME);
                }
            });

            // Call with same flags twice: equivalent to:
            //      term = _index['int_payload_field'].get('b', _POSITIONS | _FREQUENCIES);return _index['int_payload_field']['b'].tf();
            scripts.put("Call with same flags twice", vars -> {
                LeafIndexLookup leafIndexLookup = (LeafIndexLookup) vars.get("_index");
                IndexField indexField = leafIndexLookup.get("int_payload_field");

                // 1st call
                indexField.get("b", IndexLookup.FLAG_POSITIONS | IndexLookup.FLAG_FREQUENCIES);
                try {
                    // 2nd call, must throws an exception
                    return indexField.get("b").tf();
                } catch (IOException e) {
                    throw new ScriptException(e.getMessage(), e, emptyList(), "Call with same flags twice", CustomScriptPlugin.NAME);
                }
            });

            // get the number of all docs
            scripts.put("_index.numDocs()",
                    vars -> ((LeafIndexLookup) vars.get("_index")).numDocs());

            // get the number of docs with field float_payload_field
            scripts.put("_index['float_payload_field'].docCount()",
                    vars -> indexFieldScript(vars, "float_payload_field", indexField -> {
                        try {
                            return indexField.docCount();
                        } catch (IOException e) {
                            throw new ScriptException(e.getMessage(), e, emptyList(), "docCount()", CustomScriptPlugin.NAME);
                        }
                    }));

            // corner case: what if the field does not exist?
            scripts.put("_index['non_existent_field'].docCount()",
                    vars -> indexFieldScript(vars, "non_existent_field", indexField -> {
                        try {
                            return indexField.docCount();
                        } catch (IOException e) {
                            throw new ScriptException(e.getMessage(), e, emptyList(), "docCount()", CustomScriptPlugin.NAME);
                        }
                    }));

            // get the number of all tokens in all docs
            scripts.put("_index['float_payload_field'].sumttf()",
                    vars -> indexFieldScript(vars, "float_payload_field", indexField -> {
                        try {
                            return indexField.sumttf();
                        } catch (IOException e) {
                            throw new ScriptException(e.getMessage(), e, emptyList(), "sumttf()", CustomScriptPlugin.NAME);
                        }
                    }));

            // corner case get the number of all tokens in all docs for non existent
            // field
            scripts.put("_index['non_existent_field'].sumttf()",
                    vars -> indexFieldScript(vars, "non_existent_field", indexField -> {
                        try {
                            return indexField.sumttf();
                        } catch (IOException e) {
                            throw new ScriptException(e.getMessage(), e, emptyList(), "sumttf()", CustomScriptPlugin.NAME);
                        }
                    }));

            // get the sum of doc freqs in all docs
            scripts.put("_index['float_payload_field'].sumdf()",
                    vars -> indexFieldScript(vars, "float_payload_field", indexField -> {
                        try {
                            return indexField.sumdf();
                        } catch (IOException e) {
                            throw new ScriptException(e.getMessage(), e, emptyList(), "sumdf()", CustomScriptPlugin.NAME);
                        }
                    }));

            // get the sum of doc freqs in all docs for non existent field
            scripts.put("_index['non_existent_field'].sumdf()",
                    vars -> indexFieldScript(vars, "non_existent_field", indexField -> {
                        try {
                            return indexField.sumdf();
                        } catch (IOException e) {
                            throw new ScriptException(e.getMessage(), e, emptyList(), "sumdf()", CustomScriptPlugin.NAME);
                        }
                    }));

            // check term frequencies for 'a'
            scripts.put("term = _index['float_payload_field']['a']; if (term != null) {term.tf()}",
                    vars -> indexFieldTermScript(vars, "float_payload_field", "a", indexFieldTerm -> {
                        try {
                            if (indexFieldTerm != null) {
                                return indexFieldTerm.tf();
                            }
                        } catch (IOException e) {
                            throw new ScriptException(e.getMessage(), e, emptyList(), "term.tf()", CustomScriptPlugin.NAME);
                        }
                        return null;
                    }));

            // check doc frequencies for 'c'
            scripts.put("term = _index['float_payload_field']['c']; if (term != null) {term.df()}",
                    vars -> indexFieldTermScript(vars, "float_payload_field", "c", indexFieldTerm -> {
                        try {
                            if (indexFieldTerm != null) {
                                return indexFieldTerm.df();
                            }
                        } catch (IOException e) {
                            throw new ScriptException(e.getMessage(), e, emptyList(), "term.df()", CustomScriptPlugin.NAME);
                        }
                        return null;
                    }));

            // check doc frequencies for term that does not exist
            scripts.put("term = _index['float_payload_field']['non_existent_term']; if (term != null) {term.df()}",
                    vars -> indexFieldTermScript(vars, "float_payload_field", "non_existent_term", indexFieldTerm -> {
                        try {
                            if (indexFieldTerm != null) {
                                return indexFieldTerm.df();
                            }
                        } catch (IOException e) {
                            throw new ScriptException(e.getMessage(), e, emptyList(), "term.df()", CustomScriptPlugin.NAME);
                        }
                        return null;
                    }));

            // check doc frequencies for term that does not exist
            scripts.put("term = _index['non_existent_field']['non_existent_term']; if (term != null) {term.tf()}",
                    vars -> indexFieldTermScript(vars, "non_existent_field", "non_existent_term", indexFieldTerm -> {
                        try {
                            if (indexFieldTerm != null) {
                                return indexFieldTerm.tf();
                            }
                        } catch (IOException e) {
                            throw new ScriptException(e.getMessage(), e, emptyList(), "term.tf()", CustomScriptPlugin.NAME);
                        }
                        return null;
                    }));

            // check total term frequencies for 'a'
            scripts.put("term = _index['float_payload_field']['a']; if (term != null) {term.ttf()}",
                    vars -> indexFieldTermScript(vars, "float_payload_field", "a", indexFieldTerm -> {
                        try {
                            if (indexFieldTerm != null) {
                                return indexFieldTerm.ttf();
                            }
                        } catch (IOException e) {
                            throw new ScriptException(e.getMessage(), e, emptyList(), "term.ttf()", CustomScriptPlugin.NAME);
                        }
                        return null;
                    }));

            return scripts;
        }

        @SuppressWarnings("unchecked")
        static Object indexFieldScript(Map<String, Object> vars, String fieldName, Function<IndexField, Object> fn) {
            LeafIndexLookup leafIndexLookup = (LeafIndexLookup) vars.get("_index");
            return fn.apply(leafIndexLookup.get(fieldName));
        }

        @SuppressWarnings("unchecked")
        static Object indexFieldTermScript(Map<String, Object> vars, String fieldName, String term, Function<IndexFieldTerm, Object> fn) {
            return indexFieldScript(vars, fieldName, indexField -> fn.apply(indexField.get(term)));
        }

        @SuppressWarnings("unchecked")
        static Object tf(Map<String, Object> vars, String fieldName, String term) {
            return indexFieldTermScript(vars, fieldName, term, indexFieldTerm -> {
                try {
                    return indexFieldTerm.tf();
                } catch (IOException e) {
                    throw new RuntimeException("Mocked script error when retrieving TF for [" + fieldName + "][" + term + "]");
                }
            });
        }

        // Sum the payloads for a given field term, equivalent to:
        //      term = _index['float_payload_field'].get('b', _FREQUENCIES|_OFFSETS|_PAYLOADS|_POSITIONS|_CACHE);
        //      payloadSum=0;
        //      for (pos in term) {
        //          payloadSum += pos.payloadAsInt(0)
        //      };
        //      return payloadSum;
        @SuppressWarnings("unchecked")
        static Object payloadSum(Map<String, Object> vars, String fieldName, String term) {
            return indexFieldScript(vars, fieldName, indexField -> {
                IndexFieldTerm indexFieldTerm = indexField.get(term, IndexLookup.FLAG_FREQUENCIES
                        | IndexLookup.FLAG_OFFSETS
                        | IndexLookup.FLAG_PAYLOADS
                        | IndexLookup.FLAG_POSITIONS
                        | IndexLookup.FLAG_CACHE);
                int payloadSum = 0;
                for (TermPosition position : indexFieldTerm) {
                    payloadSum += position.payloadAsInt(0);
                }
                return payloadSum;
            });
        }

        @SuppressWarnings("unchecked")
        static List<Object> createPositionsArrayScriptGetInfoObjectTwice(Map<String, Object> vars, String term, int flags,
                                                                         Function<TermPosition, Object> fn) {
            LeafIndexLookup leafIndexLookup = (LeafIndexLookup) vars.get("_index");
            IndexField indexField = leafIndexLookup.get("int_payload_field");

            // 1st call
            IndexFieldTerm indexFieldTerm = indexField.get(term, flags);

            List<Object> array = new ArrayList<>();
            for (TermPosition position : indexFieldTerm) {
                array.add(fn.apply(position));
            }

            // 2nd call
            indexField.get(term, flags);

            array = new ArrayList<>();
            for (TermPosition position : indexFieldTerm) {
                array.add(fn.apply(position));
            }

            return array;
        }

        @SuppressWarnings("unchecked")
        static List<Object> createPositionsArrayScriptIterateTwice(Map<String, Object> vars, String term, int flags,
                                                                   Function<TermPosition, Object> fn) {
            LeafIndexLookup leafIndexLookup = (LeafIndexLookup) vars.get("_index");
            IndexField indexField = leafIndexLookup.get("int_payload_field");

            IndexFieldTerm indexFieldTerm = indexField.get(term, flags);

            // 1st iteration
            List<Object> array = new ArrayList<>();
            for (TermPosition position : indexFieldTerm) {
                array.add(fn.apply(position));
            }

            // 2nd iteration
            array = new ArrayList<>();
            for (TermPosition position : indexFieldTerm) {
                array.add(fn.apply(position));
            }

            return array;
        }

        @SuppressWarnings("unchecked")
        static List<Object> createPositionsArrayScript(Map<String, Object> vars, String field, String term, int flags,
                                                       Function<TermPosition, Object> fn) {

            LeafIndexLookup leafIndexLookup = (LeafIndexLookup) vars.get("_index");
            IndexField indexField = leafIndexLookup.get(field);

            IndexFieldTerm indexFieldTerm = indexField.get(term, flags);
            List<Object> array = new ArrayList<>();
            for (TermPosition position : indexFieldTerm) {
                array.add(fn.apply(position));
            }
            return array;
        }
    }

    void initTestData() throws InterruptedException, ExecutionException, IOException {
        HashMap<String, List<Object>> emptyArray = new HashMap<>();
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

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("type1")
                        .startObject("properties")
                            .startObject("int_payload_field")
                                .field("type", "text")
                                .field("index_options", "offsets")
                                .field("analyzer", "payload_int")
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        assertAcked(prepareCreate("test").addMapping("type1", mapping).setSettings(
                Settings.builder()
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

    public void testTwoScripts() throws Exception {
        initTestData();

        Script scriptFieldScript = createScript("term = _index['int_payload_field']['c']; term.tf()");
        Script scoreScript = createScript("term = _index['int_payload_field']['b']; term.tf()");
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

    public void testCallWithDifferentFlagsFails() throws Exception {
        initTestData();
        final int numPrimaries = getNumShards("test").numPrimaries;
        final String expectedError = "You must call get with all required flags! " +
                "Instead of  _index['int_payload_field'].get('b', _FREQUENCIES) and _index['int_payload_field'].get('b', _POSITIONS)" +
                " call  _index['int_payload_field'].get('b', _FREQUENCIES | _POSITIONS)  once]";

        // should throw an exception, we cannot call with different flags twice
        // if the flags of the second call were not included in the first call.
        Script script = createScript("Call with different flags twice");
        try {
            SearchResponse response = client().prepareSearch("test")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addScriptField("tvtest", script)
                    .get();

            assertThat(numPrimaries, greaterThan(1));
            assertThat(response.getFailedShards(), greaterThanOrEqualTo(1));

            for (ShardSearchFailure failure : response.getShardFailures()) {
                assertThat(failure.reason(), containsString(expectedError));
            }
        } catch (SearchPhaseExecutionException e) {
            assertThat(numPrimaries, equalTo(1));
            assertThat(e.toString(), containsString(expectedError));
        }

        // Should not throw an exception this way round
        script = createScript("Call with same flags twice");
        assertThat(client().prepareSearch("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .addScriptField("tvtest", script)
                .get().getHits().getTotalHits(), greaterThan(0L));
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

    public void testDocumentationExample() throws Exception {
        initTestData();

        Script script = createScript("Sum the payloads of [float_payload_field][b]");

        // non existing field: sum should be 0
        HashMap<String, Object> zeroArray = new HashMap<>();
        zeroArray.put("1", 0);
        zeroArray.put("2", 0);
        zeroArray.put("3", 0);
        checkValueInEachDoc(script, zeroArray, 3);

        script = createScript("Sum the payloads of [int_payload_field][b]");

        // existing field: sums should be as here:
        zeroArray.put("1", 5);
        zeroArray.put("2", 3);
        zeroArray.put("3", 1);
        checkValueInEachDoc(script, zeroArray, 3);
    }

    public void testIteratorAndRecording() throws Exception {
        initTestData();

        // call twice with record: should work as expected
        Script script = createPositionsArrayScriptIterateTwice("b", INCLUDE_ALL, "position");
        checkArrayValsInEachDoc(script, expectedPositionsArray, 3);
        script = createPositionsArrayScriptIterateTwice("b", INCLUDE_ALL, "startOffset");
        checkArrayValsInEachDoc(script, expectedStartOffsetsArray, 3);
        script = createPositionsArrayScriptIterateTwice("b", INCLUDE_ALL, "endOffset");
        checkArrayValsInEachDoc(script, expectedEndOffsetsArray, 3);
        script = createPositionsArrayScriptIterateTwice("b", INCLUDE_ALL, "payloadAsInt(-1)");
        checkArrayValsInEachDoc(script, expectedPayloadsArray, 3);

        // no record and get iterator twice: should fail
        script = createPositionsArrayScriptIterateTwice("b", INCLUDE_ALL_BUT_CACHE, "position");
        checkExceptions(script);
        script = createPositionsArrayScriptIterateTwice("b", INCLUDE_ALL_BUT_CACHE, "startOffset");
        checkExceptions(script);
        script = createPositionsArrayScriptIterateTwice("b", INCLUDE_ALL_BUT_CACHE, "endOffset");
        checkExceptions(script);
        script = createPositionsArrayScriptIterateTwice("b", INCLUDE_ALL_BUT_CACHE, "payloadAsInt(-1)");
        checkExceptions(script);

        // no record and get termObject twice and iterate: should fail
        script = createPositionsArrayScriptGetInfoObjectTwice("b", INCLUDE_ALL_BUT_CACHE, "position");
        checkExceptions(script);
        script = createPositionsArrayScriptGetInfoObjectTwice("b", INCLUDE_ALL_BUT_CACHE, "startOffset");
        checkExceptions(script);
        script = createPositionsArrayScriptGetInfoObjectTwice("b", INCLUDE_ALL_BUT_CACHE, "endOffset");
        checkExceptions(script);
        script = createPositionsArrayScriptGetInfoObjectTwice("b", INCLUDE_ALL_BUT_CACHE, "payloadAsInt(-1)");
        checkExceptions(script);

    }

    private Script createPositionsArrayScriptGetInfoObjectTwice(String term, String flags, String what) {
        return createScript("createPositionsArrayScriptGetInfoObjectTwice[" + term + "," + flags + "," + what + "]");
    }

    private Script createPositionsArrayScriptIterateTwice(String term, String flags, String what) {
        return createScript("createPositionsArrayScriptIterateTwice[" + term + "," + flags + "," + what + "]");
    }

    private Script createPositionsArrayScript(String field, String term, String flags, String what) {
        return createScript("createPositionsArrayScript[" + field + ","  + term + "," + flags + "," + what + "]");
    }

    private Script createPositionsArrayScriptDefaultGet(String field, String term, String what) {
        return createScript("createPositionsArrayScriptDefaultGet[" + field + ","  + term + "," + what + "]");
    }

    private Script createScript(String script) {
        return new Script(script, ScriptType.INLINE, CustomScriptPlugin.NAME, null);
    }

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
        String allFlags = "_POSITIONS|_OFFSETS|_PAYLOADS";
        script = createPositionsArrayScript("int_payload_field", "b", allFlags, "position");
        checkArrayValsInEachDoc(script, expectedPositionsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", allFlags, "startOffset");
        checkArrayValsInEachDoc(script, expectedStartOffsetsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", allFlags, "endOffset");
        checkArrayValsInEachDoc(script, expectedEndOffsetsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", allFlags, "payloadAsInt(-1)");
        checkArrayValsInEachDoc(script, expectedPayloadsArray, 3);

        // check all flags without record
        script = createPositionsArrayScript("int_payload_field", "b", INCLUDE_ALL_BUT_CACHE, "position");
        checkArrayValsInEachDoc(script, expectedPositionsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", INCLUDE_ALL_BUT_CACHE, "startOffset");
        checkArrayValsInEachDoc(script, expectedStartOffsetsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", INCLUDE_ALL_BUT_CACHE, "endOffset");
        checkArrayValsInEachDoc(script, expectedEndOffsetsArray, 3);
        script = createPositionsArrayScript("int_payload_field", "b", INCLUDE_ALL_BUT_CACHE, "payloadAsInt(-1)");
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

    public void testAllExceptPosAndOffset() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("type1").startObject("properties")
                .startObject("float_payload_field").field("type", "text").field("index_options", "offsets").field("term_vector", "no")
                .field("analyzer", "payload_float").endObject().startObject("string_payload_field").field("type", "text")
                .field("index_options", "offsets").field("term_vector", "no").field("analyzer", "payload_string").endObject()
                .startObject("int_payload_field").field("type", "text").field("index_options", "offsets")
                .field("analyzer", "payload_int").endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").addMapping("type1", mapping).setSettings(
                Settings.builder()
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
        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource("float_payload_field", "a|1 b|2 a|3 b "), client()
                        .prepareIndex("test", "type1", "2").setSource("string_payload_field", "a|a b|b a|a b "),
                client().prepareIndex("test", "type1", "3").setSource("float_payload_field", "a|4 b|5 a|6 b "),
                client().prepareIndex("test", "type1", "4").setSource("string_payload_field", "a|b b|a a|b b "),
                client().prepareIndex("test", "type1", "5").setSource("float_payload_field", "c "),
                client().prepareIndex("test", "type1", "6").setSource("int_payload_field", "c|1"));

        // get the number of all docs
        Script script = createScript("_index.numDocs()");
        checkValueInEachDoc(6, script, 6);

        // get the number of docs with field float_payload_field
        script = createScript("_index['float_payload_field'].docCount()");
        checkValueInEachDoc(3, script, 6);

        // corner case: what if the field does not exist?
        script = createScript("_index['non_existent_field'].docCount()");
        checkValueInEachDoc(0, script, 6);

        // get the number of all tokens in all docs
        script = createScript("_index['float_payload_field'].sumttf()");
        checkValueInEachDoc(9, script, 6);

        // corner case get the number of all tokens in all docs for non existent
        // field
        script = createScript("_index['non_existent_field'].sumttf()");
        checkValueInEachDoc(0, script, 6);

        // get the sum of doc freqs in all docs
        script = createScript("_index['float_payload_field'].sumdf()");
        checkValueInEachDoc(5, script, 6);

        // get the sum of doc freqs in all docs for non existent field
        script = createScript("_index['non_existent_field'].sumdf()");
        checkValueInEachDoc(0, script, 6);

        // check term frequencies for 'a'
        script = createScript("term = _index['float_payload_field']['a']; if (term != null) {term.tf()}");
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
        script = createScript("term = _index['float_payload_field']['c']; if (term != null) {term.df()}");
        expectedResults.put("1", 1L);
        expectedResults.put("2", 1L);
        expectedResults.put("3", 1L);
        expectedResults.put("4", 1L);
        expectedResults.put("5", 1L);
        expectedResults.put("6", 1L);
        checkValueInEachDoc(script, expectedResults, 6);
        expectedResults.clear();

        // check doc frequencies for term that does not exist
        script = createScript("term = _index['float_payload_field']['non_existent_term']; if (term != null) {term.df()}");
        expectedResults.put("1", 0L);
        expectedResults.put("2", 0L);
        expectedResults.put("3", 0L);
        expectedResults.put("4", 0L);
        expectedResults.put("5", 0L);
        expectedResults.put("6", 0L);
        checkValueInEachDoc(script, expectedResults, 6);
        expectedResults.clear();

        // check doc frequencies for term that does not exist
        script = createScript("term = _index['non_existent_field']['non_existent_term']; if (term != null) {term.tf()}");
        expectedResults.put("1", 0);
        expectedResults.put("2", 0);
        expectedResults.put("3", 0);
        expectedResults.put("4", 0);
        expectedResults.put("5", 0);
        expectedResults.put("6", 0);
        checkValueInEachDoc(script, expectedResults, 6);
        expectedResults.clear();

        // check total term frequencies for 'a'
        script = createScript("term = _index['float_payload_field']['a']; if (term != null) {term.ttf()}");
        expectedResults.put("1", 4L);
        expectedResults.put("2", 4L);
        expectedResults.put("3", 4L);
        expectedResults.put("4", 4L);
        expectedResults.put("5", 4L);
        expectedResults.put("6", 4L);
        checkValueInEachDoc(script, expectedResults, 6);
        expectedResults.clear();

        // check float payload for 'b'
        HashMap<String, List<Object>> expectedPayloadsArray = new HashMap<>();
        script = createPositionsArrayScript("float_payload_field", "b", INCLUDE_ALL, "payloadAsFloat(-1)");
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
        script = createPositionsArrayScript("string_payload_field", "b", INCLUDE_ALL, "payloadAsString()");
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
        script = createPositionsArrayScript("int_payload_field", "c", INCLUDE_ALL, "payloadAsInt(-1)");
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
                assertThat(result, equalTo(value));
            } else if (result instanceof Long) {
                assertThat(((Long) result).intValue(), equalTo(value));
            } else {
                fail();
            }
        }
    }
}
