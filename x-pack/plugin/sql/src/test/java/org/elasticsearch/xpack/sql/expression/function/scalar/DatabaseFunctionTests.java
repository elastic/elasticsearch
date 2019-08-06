/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.TestUtils;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.Project;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.type.TypesTests;
import org.elasticsearch.xpack.sql.util.DateUtils;

public class DatabaseFunctionTests extends ESTestCase {

    public void testDatabaseFunctionOutput() {
        String clusterName = randomAlphaOfLengthBetween(1, 15);
        SqlParser parser = new SqlParser();
        EsIndex test = new EsIndex("test", TypesTests.loadMapping("mapping-basic.json", true));
        Analyzer analyzer = new Analyzer(new FunctionRegistry(), new Verifier(new Metrics()));

        Configuration cfg = new Configuration(DateUtils.UTC, Protocol.FETCH_SIZE, Protocol.REQUEST_TIMEOUT, Protocol.PAGE_TIMEOUT, null,
                randomFrom(Mode.values()), randomAlphaOfLength(10), null, clusterName, randomBoolean(), randomBoolean());
        
        Project result = (Project) TestUtils.withContext(cfg, IndexResolution.valid(test),
                () -> analyzer.analyze(parser.createStatement("SELECT DATABASE()"), true));
        assertTrue(result.projections().get(0) instanceof Database);
        assertEquals(clusterName, ((Database) result.projections().get(0)).fold());
    }
}
