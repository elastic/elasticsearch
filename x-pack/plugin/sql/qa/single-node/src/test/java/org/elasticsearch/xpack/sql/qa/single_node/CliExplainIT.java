/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.qa.single_node;

import org.elasticsearch.xpack.sql.qa.cli.CliIntegrationTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class CliExplainIT extends CliIntegrationTestCase {
    public void testExplainBasic() throws IOException {
        index("test", body -> body.field("test_field", "test_value"));

        assertThat(command("EXPLAIN (PLAN PARSED) SELECT * FROM test"), containsString("plan"));
        assertThat(readLine(), startsWith("----------"));
        assertThat(readLine(), startsWith("With[{}]"));
        assertThat(readLine(), startsWith("\\_Project[[?* AS ?]]"));
        assertThat(readLine(), startsWith("  \\_UnresolvedRelation[test]"));
        assertEquals("", readLine());

        assertThat(command("EXPLAIN " + (randomBoolean() ? "" : "(PLAN ANALYZED) ") + "SELECT * FROM test"), containsString("plan"));
        assertThat(readLine(), startsWith("----------"));
        assertThat(readLine(), startsWith("Project[[test.test_field{f}#"));
        assertThat(readLine(), startsWith("\\_EsRelation[test][test_field{f}#"));
        assertEquals("", readLine());

        assertThat(command("EXPLAIN (PLAN OPTIMIZED) SELECT * FROM test"), containsString("plan"));
        assertThat(readLine(), startsWith("----------"));
        assertThat(readLine(), startsWith("Project[[test.test_field{f}#"));
        assertThat(readLine(), startsWith("\\_EsRelation[test][test_field{f}#"));
        assertEquals("", readLine());

        // TODO in this case we should probably remove the source filtering entirely. Right? It costs but we don't need it.
        assertThat(command("EXPLAIN (PLAN EXECUTABLE) SELECT * FROM test"), containsString("plan"));
        assertThat(readLine(), startsWith("----------"));
        assertThat(readLine(), startsWith("EsQueryExec[test,{"));
        assertThat(readLine(), startsWith("  \"_source\" : {"));
        assertThat(readLine(), startsWith("    \"includes\" : ["));
        assertThat(readLine(), startsWith("      \"test_field\""));
        assertThat(readLine(), startsWith("    ],"));
        assertThat(readLine(), startsWith("    \"excludes\" : [ ]"));
        assertThat(readLine(), startsWith("  },"));
        assertThat(readLine(), startsWith("  \"sort\" : ["));
        assertThat(readLine(), startsWith("    {"));
        assertThat(readLine(), startsWith("      \"_doc\" :"));
        assertThat(readLine(), startsWith("        \"order\" : \"asc\""));
        assertThat(readLine(), startsWith("      }"));
        assertThat(readLine(), startsWith("    }"));
        assertThat(readLine(), startsWith("  ]"));
        assertThat(readLine(), startsWith("}]"));
        assertEquals("", readLine());
    }

    public void testExplainWithWhere() throws IOException {
        index("test", body -> body.field("test_field", "test_value1").field("i", 1));
        index("test", body -> body.field("test_field", "test_value2").field("i", 2));

        assertThat(command("EXPLAIN (PLAN PARSED) SELECT * FROM test WHERE i = 2"), containsString("plan"));
        assertThat(readLine(), startsWith("----------"));
        assertThat(readLine(), startsWith("With[{}]"));
        assertThat(readLine(), startsWith("\\_Project[[?* AS ?]]"));
        assertThat(readLine(), startsWith("  \\_Filter[?i == 2[INTEGER]]"));
        assertThat(readLine(), startsWith("    \\_UnresolvedRelation[test]"));
        assertEquals("", readLine());

        assertThat(command("EXPLAIN " + (randomBoolean() ? "" : "(PLAN ANALYZED) ") + "SELECT * FROM test WHERE i = 2"),
                containsString("plan"));
        assertThat(readLine(), startsWith("----------"));
        assertThat(readLine(), startsWith("Project[[test.i{f}#"));
        assertThat(readLine(), startsWith("\\_Filter[test.i{f}#"));
        assertThat(readLine(), startsWith("  \\_EsRelation[test][i{f}#"));
        assertEquals("", readLine());

        assertThat(command("EXPLAIN (PLAN OPTIMIZED) SELECT * FROM test WHERE i = 2"), containsString("plan"));
        assertThat(readLine(), startsWith("----------"));
        assertThat(readLine(), startsWith("Project[[test.i{f}#"));
        assertThat(readLine(), startsWith("\\_Filter[test.i{f}"));
        assertThat(readLine(), startsWith("  \\_EsRelation[test][i{f}#"));
        assertEquals("", readLine());

        assertThat(command("EXPLAIN (PLAN EXECUTABLE) SELECT * FROM test WHERE i = 2"), containsString("plan"));
        assertThat(readLine(), startsWith("----------"));
        assertThat(readLine(), startsWith("EsQueryExec[test,{"));
        assertThat(readLine(), startsWith("  \"query\" : {"));
        assertThat(readLine(), startsWith("    \"term\" : {"));
        assertThat(readLine(), startsWith("      \"i\" : {"));
        assertThat(readLine(), startsWith("        \"value\" : 2,"));
        assertThat(readLine(), startsWith("        \"boost\" : 1.0"));
        assertThat(readLine(), startsWith("      }"));
        assertThat(readLine(), startsWith("    }"));
        assertThat(readLine(), startsWith("  },"));
        assertThat(readLine(), startsWith("  \"_source\" : {"));
        assertThat(readLine(), startsWith("    \"includes\" : ["));
        assertThat(readLine(), startsWith("      \"i\""));
        assertThat(readLine(), startsWith("      \"test_field\""));
        assertThat(readLine(), startsWith("    ],"));
        assertThat(readLine(), startsWith("    \"excludes\" : [ ]"));
        assertThat(readLine(), startsWith("  },"));
        assertThat(readLine(), startsWith("  \"sort\" : ["));
        assertThat(readLine(), startsWith("    {"));
        assertThat(readLine(), startsWith("      \"_doc\" :"));
        assertThat(readLine(), startsWith("        \"order\" : \"asc\""));
        assertThat(readLine(), startsWith("      }"));
        assertThat(readLine(), startsWith("    }"));
        assertThat(readLine(), startsWith("  ]"));
        assertThat(readLine(), startsWith("}]"));
        assertEquals("", readLine());
    }

    public void testExplainWithCount() throws IOException {
        index("test", body -> body.field("test_field", "test_value1").field("i", 1));
        index("test", body -> body.field("test_field", "test_value2").field("i", 2));

        assertThat(command("EXPLAIN (PLAN PARSED) SELECT COUNT(*) FROM test"), containsString("plan"));
        assertThat(readLine(), startsWith("----------"));
        assertThat(readLine(), startsWith("With[{}]"));
        assertThat(readLine(), startsWith("\\_Project[[?COUNT[?*] AS ?]]"));
        assertThat(readLine(), startsWith("  \\_UnresolvedRelation[test]"));
        assertEquals("", readLine());

        assertThat(command("EXPLAIN " + (randomBoolean() ? "" : "(PLAN ANALYZED) ") + "SELECT COUNT(*) FROM test"),
                containsString("plan"));
        assertThat(readLine(), startsWith("----------"));
        assertThat(readLine(), startsWith("Aggregate[[],[COUNT(*)"));
        assertThat(readLine(), startsWith("\\_EsRelation[test][i{f}#"));
        assertEquals("", readLine());

        assertThat(command("EXPLAIN (PLAN OPTIMIZED) SELECT COUNT(*) FROM test"), containsString("plan"));
        assertThat(readLine(), startsWith("----------"));
        assertThat(readLine(), startsWith("Aggregate[[],[COUNT(*)"));
        assertThat(readLine(), startsWith("\\_EsRelation[test][i{f}#"));
        assertEquals("", readLine());

        assertThat(command("EXPLAIN (PLAN EXECUTABLE) SELECT COUNT(*) FROM test"), containsString("plan"));
        assertThat(readLine(), startsWith("----------"));
        assertThat(readLine(), startsWith("EsQueryExec[test,{"));
        assertThat(readLine(), startsWith("  \"size\" : 0,"));
        assertThat(readLine(), startsWith("  \"_source\" : false,"));
        assertThat(readLine(), startsWith("  \"stored_fields\" : \"_none_\","));
        assertThat(readLine(), startsWith("  \"sort\" : ["));
        assertThat(readLine(), startsWith("    {"));
        assertThat(readLine(), startsWith("      \"_doc\" :"));
        assertThat(readLine(), startsWith("        \"order\" : \"asc\""));
        assertThat(readLine(), startsWith("      }"));
        assertThat(readLine(), startsWith("    }"));
        assertThat(readLine(), startsWith("  ]"));
        assertThat(readLine(), startsWith("  \"track_total_hits\" : 2147483647"));
        assertThat(readLine(), startsWith("}]"));
        assertEquals("", readLine());
    }
}
