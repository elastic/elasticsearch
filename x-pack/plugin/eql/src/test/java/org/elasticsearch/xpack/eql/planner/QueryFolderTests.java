/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.analysis.Analyzer;
import org.elasticsearch.xpack.eql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.eql.analysis.Verifier;
import org.elasticsearch.xpack.eql.expression.function.EqlFunctionRegistry;
import org.elasticsearch.xpack.eql.optimizer.Optimizer;
import org.elasticsearch.xpack.eql.parser.EqlParser;
import org.elasticsearch.xpack.eql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;

import java.util.Map;

import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.TypesTests.loadMapping;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class QueryFolderTests extends ESTestCase {

    private EqlParser parser = new EqlParser();
    private PreAnalyzer preAnalyzer = new PreAnalyzer();
    private Analyzer analyzer = new Analyzer(new EqlFunctionRegistry(), new Verifier());
    private Optimizer optimizer = new Optimizer();
    private Planner planner = new Planner();

    private IndexResolution index = IndexResolution.valid(new EsIndex("test", loadMapping("mapping-default.json")));


    private PhysicalPlan plan(IndexResolution resolution, String eql) {
        return planner.plan(optimizer.optimize(analyzer.analyze(preAnalyzer.preAnalyze(parser.createStatement(eql), resolution))));
    }

    private PhysicalPlan plan(String eql) {
        return plan(index, eql);
    }

    private void checkSuccess(String eql, String expectedQuery) {
        PhysicalPlan p = plan(eql);
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        assertEquals(23, eqe.output().size());
        assertEquals(KEYWORD, eqe.output().get(0).dataType());

        String generatedQuery = eqe.queryContainer().toString().replaceAll("\\s+", "");

        assertThat(generatedQuery, containsString("\"_source\":{\"includes\":[],\"excludes\":[]"));

        final Map<String, Object> generated = XContentHelper.convertToMap(XContentType.JSON.xContent(), generatedQuery, true);
        final Map<String, Object> expected = XContentHelper.convertToMap(XContentType.JSON.xContent(), expectedQuery, true);

        assertThat(expected, is(generated.get("query")));
    }

    public void testBasicPlan() {
        checkSuccess("process where true",
                "{\n" +
                        "    \"term\" : {\n" +
                        "      \"event.category\" : {\n" +
                        "        \"value\" : \"process\",\n" +
                        "        \"boost\" : 1.0\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }");
    }

    public void testSimpleNumericFiler() {
        checkSuccess("process where serial_event_id = 1",
                "{\n" +
                        "        \"bool\": {\n" +
                        "            \"must\": [\n" +
                        "                {\n" +
                        "                    \"term\": {\n" +
                        "                        \"event.category\": {\n" +
                        "                            \"value\": \"process\",\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"term\": {\n" +
                        "                        \"serial_event_id\": {\n" +
                        "                            \"value\": 1,\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"boost\": 1.0\n" +
                        "        }\n" +
                        "    }");

        checkSuccess("process where serial_event_id < 4",
                "{\n" +
                        "        \"bool\": {\n" +
                        "            \"must\": [\n" +
                        "                {\n" +
                        "                    \"term\": {\n" +
                        "                        \"event.category\": {\n" +
                        "                            \"value\": \"process\",\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"range\": {\n" +
                        "                        \"serial_event_id\": {\n" +
                        "                            \"from\": null,\n" +
                        "                            \"to\": 4,\n" +
                        "                            \"include_lower\": false,\n" +
                        "                            \"include_upper\": false,\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"boost\": 1.0\n" +
                        "        }\n" +
                        "    }");

        checkSuccess("process where serial_event_id <= 4",
                "{\n" +
                        "        \"bool\": {\n" +
                        "            \"must\": [\n" +
                        "                {\n" +
                        "                    \"term\": {\n" +
                        "                        \"event.category\": {\n" +
                        "                            \"value\": \"process\",\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"range\": {\n" +
                        "                        \"serial_event_id\": {\n" +
                        "                            \"from\": null,\n" +
                        "                            \"to\": 4,\n" +
                        "                            \"include_lower\": false,\n" +
                        "                            \"include_upper\": true,\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"boost\": 1.0\n" +
                        "        }\n" +
                        "    }");

        checkSuccess("process where serial_event_id > 4",
                "{\n" +
                        "        \"bool\": {\n" +
                        "            \"must\": [\n" +
                        "                {\n" +
                        "                    \"term\": {\n" +
                        "                        \"event.category\": {\n" +
                        "                            \"value\": \"process\",\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"range\": {\n" +
                        "                        \"serial_event_id\": {\n" +
                        "                            \"from\": 4,\n" +
                        "                            \"to\": null,\n" +
                        "                            \"include_lower\": false,\n" +
                        "                            \"include_upper\": false,\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"boost\": 1.0\n" +
                        "        }\n" +
                        "    }");

        checkSuccess("process where serial_event_id >= 4",
                "{\n" +
                        "        \"bool\": {\n" +
                        "            \"must\": [\n" +
                        "                {\n" +
                        "                    \"term\": {\n" +
                        "                        \"event.category\": {\n" +
                        "                            \"value\": \"process\",\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"range\": {\n" +
                        "                        \"serial_event_id\": {\n" +
                        "                            \"from\": 4,\n" +
                        "                            \"to\": null,\n" +
                        "                            \"include_lower\": true,\n" +
                        "                            \"include_upper\": false,\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"boost\": 1.0\n" +
                        "        }\n" +
                        "    }");
    }

    public void testMixedTypeFiler() {
        checkSuccess("process where process_name == \"notepad.exe\" or (serial_event_id < 4.5 and serial_event_id >= 3.1)",
                "{\n" +
                        "        \"bool\": {\n" +
                        "            \"must\": [\n" +
                        "                {\n" +
                        "                    \"term\": {\n" +
                        "                        \"event.category\": {\n" +
                        "                            \"value\": \"process\",\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"bool\": {\n" +
                        "                        \"should\": [\n" +
                        "                            {\n" +
                        "                                \"term\": {\n" +
                        "                                    \"process_name\": {\n" +
                        "                                        \"value\": \"notepad.exe\",\n" +
                        "                                        \"boost\": 1.0\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            },\n" +
                        "                            {\n" +
                        "                                \"range\": {\n" +
                        "                                    \"serial_event_id\": {\n" +
                        "                                        \"from\": 3.1,\n" +
                        "                                        \"to\": 4.5,\n" +
                        "                                        \"include_lower\": true,\n" +
                        "                                        \"include_upper\": false,\n" +
                        "                                        \"boost\": 1.0\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            }\n" +
                        "                        ],\n" +
                        "                        \"boost\": 1.0\n" +
                        "                    }\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"boost\": 1.0\n" +
                        "        }\n" +
                        "    }");
    }

    public void testFilterSymmetry() {
        final String expected = "{\n" +
                "        \"bool\": {\n" +
                "            \"must\": [\n" +
                "                {\n" +
                "                    \"term\": {\n" +
                "                        \"event.category\": {\n" +
                "                            \"value\": \"process\",\n" +
                "                            \"boost\": 1.0\n" +
                "                        }\n" +
                "                    }\n" +
                "                },\n" +
                "                {\n" +
                "                    \"range\": {\n" +
                "                        \"exit_code\": {\n" +
                "                            \"from\": 0,\n" +
                "                            \"to\": null,\n" +
                "                            \"include_lower\": false,\n" +
                "                            \"include_upper\": false,\n" +
                "                            \"boost\": 1.0\n" +
                "                        }\n" +
                "                    }\n" +
                "                }\n" +
                "            ],\n" +
                "            \"boost\": 1.0\n" +
                "        }\n" +
                "    }";

        checkSuccess("process where exit_code > 0", expected);
        checkSuccess("process where 0 < exit_code", expected);
    }

    public void testNotFilter() {
        checkSuccess("process where not (exit_code > -1)",
                "{\n" +
                        "        \"bool\": {\n" +
                        "            \"must\": [\n" +
                        "                {\n" +
                        "                    \"term\": {\n" +
                        "                        \"event.category\": {\n" +
                        "                            \"value\": \"process\",\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"range\": {\n" +
                        "                        \"exit_code\": {\n" +
                        "                            \"from\": null,\n" +
                        "                            \"to\": -1,\n" +
                        "                            \"include_lower\": false,\n" +
                        "                            \"include_upper\": true,\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"boost\": 1.0\n" +
                        "        }\n" +
                        "    }");
    }

    public void testPropertyEquationFilterUnsupported() {
        QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class,
                () -> plan("process where (serial_event_id<9 and serial_event_id >= 7) or (opcode == pid)"));
        String msg = e.getMessage();
        assertEquals("Line 1:74: Comparisons against variables are not (currently) supported; offender [pid] in [==]", msg);
    }

    public void testPropertyEquationInClauseFilterUnsupported() {
        QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class,
                () -> plan("process where opcode in (1,3) and process_name in (parent_process_name, \"SYSTEM\")"));
        String msg = e.getMessage();
        assertEquals("Line 1:52: Comparisons against variables are not (currently) supported; offender [parent_process_name] in [==]", msg);
    }

    // NOTE: The IN clause in the filter is pending optimization
    // https://github.com/elastic/elasticsearch/issues/52939
    public void testInClauseFilter() {
        checkSuccess("process where process_name in (\"python.exe\", \"SMSS.exe\", \"explorer.exe\")",
                "{\n" +
                        "        \"bool\": {\n" +
                        "            \"must\": [\n" +
                        "                {\n" +
                        "                    \"term\": {\n" +
                        "                        \"event.category\": {\n" +
                        "                            \"value\": \"process\",\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"bool\": {\n" +
                        "                        \"should\": [\n" +
                        "                            {\n" +
                        "                                \"bool\": {\n" +
                        "                                    \"should\": [\n" +
                        "                                        {\n" +
                        "                                            \"term\": {\n" +
                        "                                                \"process_name\": {\n" +
                        "                                                    \"value\": \"python.exe\",\n" +
                        "                                                    \"boost\": 1.0\n" +
                        "                                                }\n" +
                        "                                            }\n" +
                        "                                        },\n" +
                        "                                        {\n" +
                        "                                            \"term\": {\n" +
                        "                                                \"process_name\": {\n" +
                        "                                                    \"value\": \"SMSS.exe\",\n" +
                        "                                                    \"boost\": 1.0\n" +
                        "                                                }\n" +
                        "                                            }\n" +
                        "                                        }\n" +
                        "                                    ],\n" +
                        "                                    \"boost\": 1.0\n" +
                        "                                }\n" +
                        "                            },\n" +
                        "                            {\n" +
                        "                                \"term\": {\n" +
                        "                                    \"process_name\": {\n" +
                        "                                        \"value\": \"explorer.exe\",\n" +
                        "                                        \"boost\": 1.0\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            }\n" +
                        "                        ],\n" +
                        "                        \"boost\": 1.0\n" +
                        "                    }\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"boost\": 1.0\n" +
                        "        }\n" +
                        "    }");

        checkSuccess("process where process_path == \"*\\\\red_ttp\\\\wininit.*\" and opcode in (0,1,2,3,4)",
                "{\n" +
                        "        \"bool\": {\n" +
                        "            \"must\": [\n" +
                        "                {\n" +
                        "                    \"term\": {\n" +
                        "                        \"event.category\": {\n" +
                        "                            \"value\": \"process\",\n" +
                        "                            \"boost\": 1.0\n" +
                        "                        }\n" +
                        "                    }\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"bool\": {\n" +
                        "                        \"must\": [\n" +
                        "                            {\n" +
                        "                                \"term\": {\n" +
                        "                                    \"process_path\": {\n" +
                        "                                        \"value\": \"*\\\\red_ttp\\\\wininit.*\",\n" +
                        "                                        \"boost\": 1.0\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            },\n" +
                        "                            {\n" +
                        "                                \"bool\": {\n" +
                        "                                    \"should\": [\n" +
                        "                                        {\n" +
                        "                                            \"bool\": {\n" +
                        "                                                \"should\": [\n" +
                        "                                                    {\n" +
                        "                                                        \"bool\": {\n" +
                        "                                                            \"should\": [\n" +
                        "                                                                {\n" +
                        "                                                                    \"bool\": {\n" +
                        "                                                                        \"should\": [\n" +
                        "                                                                            {\n" +
                        "                                                                                \"term\": {\n" +
                        "                                                                                    \"opcode\": {\n" +
                        "                                                                                        \"value\": 0,\n" +
                        "                                                                                        \"boost\": 1.0\n" +
                        "                                                                                    }\n" +
                        "                                                                                }\n" +
                        "                                                                            },\n" +
                        "                                                                            {\n" +
                        "                                                                                \"term\": {\n" +
                        "                                                                                    \"opcode\": {\n" +
                        "                                                                                        \"value\": 1,\n" +
                        "                                                                                        \"boost\": 1.0\n" +
                        "                                                                                    }\n" +
                        "                                                                                }\n" +
                        "                                                                            }\n" +
                        "                                                                        ],\n" +
                        "                                                                        \"boost\": 1.0\n" +
                        "                                                                    }\n" +
                        "                                                                },\n" +
                        "                                                                {\n" +
                        "                                                                    \"term\": {\n" +
                        "                                                                        \"opcode\": {\n" +
                        "                                                                            \"value\": 2,\n" +
                        "                                                                            \"boost\": 1.0\n" +
                        "                                                                        }\n" +
                        "                                                                    }\n" +
                        "                                                                }\n" +
                        "                                                            ],\n" +
                        "                                                            \"boost\": 1.0\n" +
                        "                                                        }\n" +
                        "                                                    },\n" +
                        "                                                    {\n" +
                        "                                                        \"term\": {\n" +
                        "                                                            \"opcode\": {\n" +
                        "                                                                \"value\": 3,\n" +
                        "                                                                \"boost\": 1.0\n" +
                        "                                                            }\n" +
                        "                                                        }\n" +
                        "                                                    }\n" +
                        "                                                ],\n" +
                        "                                                \"boost\": 1.0\n" +
                        "                                            }\n" +
                        "                                        },\n" +
                        "                                        {\n" +
                        "                                            \"term\": {\n" +
                        "                                                \"opcode\": {\n" +
                        "                                                    \"value\": 4,\n" +
                        "                                                    \"boost\": 1.0\n" +
                        "                                                }\n" +
                        "                                            }\n" +
                        "                                        }\n" +
                        "                                    ],\n" +
                        "                                    \"boost\": 1.0\n" +
                        "                                }\n" +
                        "                            }\n" +
                        "                        ],\n" +
                        "                        \"boost\": 1.0\n" +
                        "                    }\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"boost\": 1.0\n" +
                        "        }\n" +
                        "    }");
    }
}
