/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.query;

import java.nio.file.Path;

public class SelectSpecTests extends CompareToH2BaseTestCase {
    public SelectSpecTests(String queryName, String query, Integer lineNumber, Path source) {
        super(queryName, query, lineNumber, source);
    }

    public static Iterable<Object[]> queries() throws Exception {
        return readScriptSpec("/org/elasticsearch/sql/jdbc/integration/query/select/select.spec");
    }
}
