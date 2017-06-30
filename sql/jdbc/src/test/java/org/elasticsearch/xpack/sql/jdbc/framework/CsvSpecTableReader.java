/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.integration.util.framework;

import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.relique.io.TableReader;

public class CsvSpecTableReader implements TableReader {

    @Override
    public Reader getReader(Statement statement, String tableName) throws SQLException {
        Reader reader = CsvInfraSuite.CSV_READERS.remove(statement.getConnection());
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
