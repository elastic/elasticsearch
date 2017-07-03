/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.elasticsearch.xpack.sql.jdbc.csv.CsvSpecIntegrationTest;
import org.relique.io.TableReader;

import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class CsvSpecTableReader implements TableReader {

    @Override
    public Reader getReader(Statement statement, String tableName) throws SQLException {
        Reader reader = CsvSpecIntegrationTest.CSV_READERS.remove(statement.getConnection());
        if (reader == null) {
            throw new RuntimeException("Cannot find reader for test " + tableName);
        }
        return reader;
    }

    @Override
    public List<String> getTableNames(Connection connection) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
