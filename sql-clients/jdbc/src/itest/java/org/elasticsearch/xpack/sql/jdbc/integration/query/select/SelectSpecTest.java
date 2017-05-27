/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.query.select;

import org.elasticsearch.xpack.sql.jdbc.integration.query.CompareToH2BaseTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class SelectSpecTest extends CompareToH2BaseTest {

    @Parameters(name = "test{0}")
    public static Iterable<Object[]> queries() throws Exception {
        return readScriptSpec("/org/elasticsearch/sql/jdbc/integration/query/select/select.spec");
    }
}
