/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class ExplainIT extends CliIntegrationTestCase {
    public void testExplainBasic() throws IOException {
        index("test", body -> body.field("test_field", "test_value"));

        command("EXPLAIN (PLAN PARSED) SELECT * FROM test");
        assertThat(in.readLine(), containsString("plan"));
        assertThat(in.readLine(), startsWith("----------"));
        assertThat(in.readLine(), startsWith("With[{}]"));
        assertThat(in.readLine(), startsWith("\\_Project[[?*]]"));
        assertThat(in.readLine(), startsWith("  \\_UnresolvedRelation[[index=test],null]"));
        assertEquals("", in.readLine());

        command("EXPLAIN " + (randomBoolean() ? "" : "(PLAN ANALYZED) ") + "SELECT * FROM test");
        assertThat(in.readLine(), containsString("plan"));
        assertThat(in.readLine(), startsWith("----------"));
        assertThat(in.readLine(), startsWith("Project[[test_field{r}#"));
        assertThat(in.readLine(), startsWith("\\_SubQueryAlias[test]"));
        assertThat(in.readLine(), startsWith("  \\_CatalogTable[test][test_field{r}#"));
        assertEquals("", in.readLine());

        command("EXPLAIN (PLAN OPTIMIZED) SELECT * FROM test");
        assertThat(in.readLine(), containsString("plan"));
        assertThat(in.readLine(), startsWith("----------"));
        assertThat(in.readLine(), startsWith("Project[[test_field{r}#"));
        assertThat(in.readLine(), startsWith("\\_CatalogTable[test][test_field{r}#"));
        assertEquals("", in.readLine());

        // TODO in this case we should probably remove the source filtering entirely. Right? It costs but we don't need it.
        command("EXPLAIN (PLAN EXECUTABLE) SELECT * FROM test");
        assertThat(in.readLine(), containsString("plan"));
        assertThat(in.readLine(), startsWith("----------"));
        assertThat(in.readLine(), startsWith("EsQueryExec[test,{"));
        assertThat(in.readLine(), startsWith("  \"_source\" : {"));
        assertThat(in.readLine(), startsWith("    \"includes\" : ["));
        assertThat(in.readLine(), startsWith("      \"test_field\""));
        assertThat(in.readLine(), startsWith("    ],"));
        assertThat(in.readLine(), startsWith("    \"excludes\" : [ ]"));
        assertThat(in.readLine(), startsWith("  }"));
        assertThat(in.readLine(), startsWith("}]"));
        assertEquals("", in.readLine());
    }

    public void testExplainWithWhere() throws IOException {
        index("test", body -> body.field("test_field", "test_value1").field("i", 1));
        index("test", body -> body.field("test_field", "test_value2").field("i", 2));

        command("EXPLAIN (PLAN PARSED) SELECT * FROM test WHERE i = 2");
        assertThat(in.readLine(), containsString("plan"));
        assertThat(in.readLine(), startsWith("----------"));
        assertThat(in.readLine(), startsWith("With[{}]"));
        assertThat(in.readLine(), startsWith("\\_Project[[?*]]"));
        assertThat(in.readLine(), startsWith("  \\_Filter[?i = 2]"));
        assertThat(in.readLine(), startsWith("    \\_UnresolvedRelation[[index=test],null]"));
        assertEquals("", in.readLine());

        command("EXPLAIN " + (randomBoolean() ? "" : "(PLAN ANALYZED) ") + "SELECT * FROM test WHERE i = 2");
        assertThat(in.readLine(), containsString("plan"));
        assertThat(in.readLine(), startsWith("----------"));
        assertThat(in.readLine(), startsWith("Project[[i{r}#"));
        assertThat(in.readLine(), startsWith("\\_Filter[i{r}#"));
        assertThat(in.readLine(), startsWith("  \\_SubQueryAlias[test]"));
        assertThat(in.readLine(), startsWith("    \\_CatalogTable[test][i{r}#"));
        assertEquals("", in.readLine());

        command("EXPLAIN (PLAN OPTIMIZED) SELECT * FROM test WHERE i = 2");
        assertThat(in.readLine(), containsString("plan"));
        assertThat(in.readLine(), startsWith("----------"));
        assertThat(in.readLine(), startsWith("Project[[i{r}#"));
        assertThat(in.readLine(), startsWith("\\_Filter[i{r}#"));
        assertThat(in.readLine(), startsWith("  \\_CatalogTable[test][i{r}#"));
        assertEquals("", in.readLine());

        command("EXPLAIN (PLAN EXECUTABLE) SELECT * FROM test WHERE i = 2");
        assertThat(in.readLine(), containsString("plan"));
        assertThat(in.readLine(), startsWith("----------"));
        assertThat(in.readLine(), startsWith("EsQueryExec[test,{"));
        assertThat(in.readLine(), startsWith("  \"query\" : {"));
        assertThat(in.readLine(), startsWith("    \"term\" : {"));
        assertThat(in.readLine(), startsWith("      \"i\" : {"));
        assertThat(in.readLine(), startsWith("        \"value\" : 2,"));
        assertThat(in.readLine(), startsWith("        \"boost\" : 1.0"));
        assertThat(in.readLine(), startsWith("      }"));
        assertThat(in.readLine(), startsWith("    }"));
        assertThat(in.readLine(), startsWith("  },"));
        assertThat(in.readLine(), startsWith("  \"_source\" : {"));
        assertThat(in.readLine(), startsWith("    \"includes\" : ["));
        assertThat(in.readLine(), startsWith("      \"test_field\""));
        assertThat(in.readLine(), startsWith("    ],"));
        assertThat(in.readLine(), startsWith("    \"excludes\" : [ ]"));
        assertThat(in.readLine(), startsWith("  },"));
        assertThat(in.readLine(), startsWith("  \"docvalue_fields\" : ["));
        assertThat(in.readLine(), startsWith("    \"i\""));
        assertThat(in.readLine(), startsWith("  ]"));
        assertThat(in.readLine(), startsWith("}]"));
        assertEquals("", in.readLine());
    }

    public void testExplainWithCount() throws IOException {
        index("test", body -> body.field("test_field", "test_value1").field("i", 1));
        index("test", body -> body.field("test_field", "test_value2").field("i", 2));

        command("EXPLAIN (PLAN PARSED) SELECT COUNT(*) FROM test");
        assertThat(in.readLine(), containsString("plan"));
        assertThat(in.readLine(), startsWith("----------"));
        assertThat(in.readLine(), startsWith("With[{}]"));
        assertThat(in.readLine(), startsWith("\\_Project[[?COUNT(?*)]]"));
        assertThat(in.readLine(), startsWith("  \\_UnresolvedRelation[[index=test],null]"));
        assertEquals("", in.readLine());

        command("EXPLAIN " + (randomBoolean() ? "" : "(PLAN ANALYZED) ") + "SELECT COUNT(*) FROM test");
        assertThat(in.readLine(), containsString("plan"));
        assertThat(in.readLine(), startsWith("----------"));
        assertThat(in.readLine(), startsWith("Aggregate[[],[COUNT(1)#"));
        assertThat(in.readLine(), startsWith("\\_SubQueryAlias[test]"));
        assertThat(in.readLine(), startsWith("  \\_CatalogTable[test][i{r}#"));
        assertEquals("", in.readLine());

        command("EXPLAIN (PLAN OPTIMIZED) SELECT COUNT(*) FROM test");
        assertThat(in.readLine(), containsString("plan"));
        assertThat(in.readLine(), startsWith("----------"));
        assertThat(in.readLine(), startsWith("Aggregate[[],[COUNT(1)#"));
        assertThat(in.readLine(), startsWith("\\_CatalogTable[test][i{r}#"));
        assertEquals("", in.readLine());

        command("EXPLAIN (PLAN EXECUTABLE) SELECT COUNT(*) FROM test");
        assertThat(in.readLine(), containsString("plan"));
        assertThat(in.readLine(), startsWith("----------"));
        assertThat(in.readLine(), startsWith("EsQueryExec[test,{"));
        assertThat(in.readLine(), startsWith("  \"size\" : 0,"));
        assertThat(in.readLine(), startsWith("  \"_source\" : false,"));
        assertThat(in.readLine(), startsWith("  \"stored_fields\" : \"_none_\""));
        assertThat(in.readLine(), startsWith("}]"));
        assertEquals("", in.readLine());
    }
}
