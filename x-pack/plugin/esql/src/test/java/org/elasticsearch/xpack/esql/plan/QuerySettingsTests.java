/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.AfterClass;

import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class QuerySettingsTests extends ESTestCase {

    public void test() {

        QuerySetting project_routing = new QuerySetting(
            Source.EMPTY,
            new Alias(Source.EMPTY, "project_routing", new Literal(Source.EMPTY, BytesRefs.toBytesRef("my-project"), DataType.KEYWORD))
        );
        QuerySettings.validate(new EsqlStatement(null, List.of(project_routing)), null);

        QuerySetting wrong_type = new QuerySetting(
            Source.EMPTY,
            new Alias(Source.EMPTY, "project_routing", new Literal(Source.EMPTY, 12, DataType.INTEGER))
        );
        assertThat(
            expectThrows(ParsingException.class, () -> QuerySettings.validate(new EsqlStatement(null, List.of(wrong_type)), null))
                .getMessage(),
            containsString("Setting [project_routing] must be of type KEYWORD")
        );

        QuerySetting non_existing = new QuerySetting(
            Source.EMPTY,
            new Alias(Source.EMPTY, "non_existing", new Literal(Source.EMPTY, BytesRefs.toBytesRef("12"), DataType.KEYWORD))
        );
        assertThat(
            expectThrows(ParsingException.class, () -> QuerySettings.validate(new EsqlStatement(null, List.of(non_existing)), null))
                .getMessage(),
            containsString("Unknown setting [non_existing]")
        );
    }

    @AfterClass
    public static void generateDocs() throws Exception {
        for (QuerySettings value : QuerySettings.values()) {
            DocsV3Support.SettingsDocsSupport settingsDocsSupport = new DocsV3Support.SettingsDocsSupport(
                value,
                QuerySettingsTests.class,
                DocsV3Support.callbacksFromSystemProperty()
            );
            settingsDocsSupport.renderDocs();
        }
    }
}
