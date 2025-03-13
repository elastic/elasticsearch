/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.sql.action.Protocol;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.types.SqlTypesTests;
import org.elasticsearch.xpack.sql.util.DateUtils;

import static org.elasticsearch.xpack.sql.analysis.analyzer.AnalyzerTestUtils.analyzer;

public class DatabaseFunctionTests extends ESTestCase {

    public void testDatabaseFunctionOutput() {
        String clusterName = randomAlphaOfLengthBetween(1, 15);
        SqlParser parser = new SqlParser();
        EsIndex test = new EsIndex("test", SqlTypesTests.loadMapping("mapping-basic.json", true));
        SqlConfiguration sqlConfig = new SqlConfiguration(
            DateUtils.UTC,
            null,
            Protocol.FETCH_SIZE,
            Protocol.REQUEST_TIMEOUT,
            Protocol.PAGE_TIMEOUT,
            null,
            null,
            randomFrom(Mode.values()),
            randomAlphaOfLength(10),
            null,
            null,
            clusterName,
            randomBoolean(),
            randomBoolean(),
            null,
            null,
            randomBoolean()
        );
        Analyzer analyzer = analyzer(sqlConfig, IndexResolution.valid(test));

        Project result = (Project) analyzer.analyze(parser.createStatement("SELECT DATABASE()"), true);
        NamedExpression ne = result.projections().get(0);
        assertTrue(ne instanceof Alias);
        assertTrue(((Alias) ne).child() instanceof Database);
        assertEquals(clusterName, ((Database) ((Alias) ne).child()).fold());
    }
}
