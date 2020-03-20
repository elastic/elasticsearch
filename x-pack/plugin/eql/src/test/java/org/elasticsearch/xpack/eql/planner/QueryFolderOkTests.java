/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.eql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.eql.plan.physical.PhysicalPlan;

import java.util.Arrays;

import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.hamcrest.Matchers.containsString;

public class QueryFolderOkTests extends AbstractQueryFolderTestCase {
    private static Object[][] specs = {
            {"basic", "process where true", null},
            {"singleNumericFilterEquals", "process where serial_event_id = 1", "\"term\":{\"serial_event_id\":{\"value\":1"},
            {"singleNumericFilterLess", "process where serial_event_id < 4",
                    "\"range\":{\"serial_event_id\":{\"from\":null,\"to\":4,\"include_lower\":false,\"include_upper\":false"
            },
            {"singleNumericFilterLessSymmetry", "process where 4 > serial_event_id",
                    "\"range\":{\"serial_event_id\":{\"from\":null,\"to\":4,\"include_lower\":false,\"include_upper\":false"
            },
            {"singleNumericFilterLessEquals", "process where serial_event_id <= 4",
                    "\"range\":{\"serial_event_id\":{\"from\":null,\"to\":4,\"include_lower\":false,\"include_upper\":true"
            },
            {"singleNumericFilterGreater", "process where serial_event_id > 4",
                    "\"range\":{\"serial_event_id\":{\"from\":4,\"to\":null,\"include_lower\":false,\"include_upper\":false"
            },
            {"singleNumericFilterGreaterEquals", "process where serial_event_id >= 4",
                    "\"range\":{\"serial_event_id\":{\"from\":4,\"to\":null,\"include_lower\":true,\"include_upper\":false"
            },
            {"mixedTypeFilter", "process where process_name == \"notepad.exe\" or (serial_event_id < 4.5 and serial_event_id >= 3.1)",
                    new Object[]{
                            "\"term\":{\"process_name\":{\"value\":\"notepad.exe\"",
                            "\"range\":{\"serial_event_id\":{\"from\":3.1,\"to\":4.5,\"include_lower\":true,\"include_upper\":false"
                    }
            },
            {"notFilter", "process where not (exit_code > -1)",
                    "\"range\":{\"exit_code\":{\"from\":null,\"to\":-1,\"include_lower\":false,\"include_upper\":true"
            },
            {"inFilter", "process where process_name in (\"python.exe\", \"SMSS.exe\", \"explorer.exe\")",
                    new Object[]{
                            "\"term\":{\"process_name\":{\"value\":\"python.exe\"",
                            "\"term\":{\"process_name\":{\"value\":\"SMSS.exe\"",
                            "\"term\":{\"process_name\":{\"value\":\"explorer.exe\"",
                    }
            },
            {"equalsAndInFilter", "process where process_path == \"*\\\\red_ttp\\\\wininit.*\" and opcode in (0,1,2,3)",
                    new Object[]{
                            "\"wildcard\":{\"process_path\":{\"wildcard\":\"*\\\\\\\\red_ttp\\\\\\\\wininit.*\"",
                            "\"term\":{\"opcode\":{\"value\":0",
                            "\"term\":{\"opcode\":{\"value\":1",
                            "\"term\":{\"opcode\":{\"value\":2",
                            "\"term\":{\"opcode\":{\"value\":3",
                    }
            },
            {"substringFunction", "process where substring(file_name, -4) == '.exe'",
                    new Object[]{
                            "{\"script\":{\"source\":\""
                            + "InternalSqlScriptUtils.nullSafeFilter("
                            + "InternalSqlScriptUtils.eq("
                            + "InternalSqlScriptUtils.substring("
                            + "InternalSqlScriptUtils.docValue(doc,params.v0),params.v1,params.v2),params.v3))",
                            "\"params\":{\"v0\":\"file_name.keyword\",\"v1\":-4,\"v2\":null,\"v3\":\".exe\"}"
                            
                    }
            }
    };

    private final String name;
    private final String query;
    private final Object expect;

    public QueryFolderOkTests(String name, String query, Object expect) {
        this.name = name;
        this.query = query;
        this.expect = expect;
    }

    @ParametersFactory(shuffle = false, argumentFormatting = "%1$s.test")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(specs);
    }

    public void test() {
        PhysicalPlan p = plan(query);
        assertEquals(EsQueryExec.class, p.getClass());
        EsQueryExec eqe = (EsQueryExec) p;
        assertEquals(23, eqe.output().size());
        assertEquals(KEYWORD, eqe.output().get(0).dataType());

        final String query = eqe.queryContainer().toString().replaceAll("\\s+", "");

        // test query term
        if (expect != null) {
            if (expect instanceof Object[]) {
                for (Object item : (Object[]) expect) {
                    assertThat(query, containsString((String) item));
                }
            } else {
                assertThat(query, containsString((String) expect));
            }
        }

        // test common term
        assertThat(query, containsString("\"term\":{\"event.category\":{\"value\":\"process\""));

        // test field source extraction
        assertThat(query, containsString("\"_source\":{\"includes\":[],\"excludes\":[]"));
    }
}
