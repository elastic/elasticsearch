/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

class JdbcStatement implements Statement, JdbcWrapper {

    final JdbcConnection con;
    final JdbcConfiguration cfg;

    private boolean closed = false;
    private boolean closeOnCompletion = false;
    private boolean ignoreResultSetClose = false;

    protected JdbcResultSet rs;
    final RequestMeta requestMeta;

    JdbcStatement(JdbcConnection jdbcConnection, JdbcConfiguration info) {
        this.con = jdbcConnection;
        this.cfg = info;
        this.requestMeta = new RequestMeta(info.pageSize(), info.pageTimeout(), info.queryTimeout());
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (!execute(sql)) {
            throw new SQLException("Invalid sql query [" +  sql + "]");
        }
        return rs;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Update not supported");
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            closeResultSet();
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkOpen();
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkOpen();
        if (max < 0) {
            throw new SQLException("Field size must be positive");
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        long result = getLargeMaxRows();
        if (result > Integer.MAX_VALUE) {
            throw new SQLException("Max rows exceeds limit of " + Integer.MAX_VALUE);
        }
        return Math.toIntExact(result);
    }


    @Override
    public long getLargeMaxRows() throws SQLException {
        checkOpen();
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        setLargeMaxRows(max);
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        checkOpen();
        if (max < 0) {
            throw new SQLException("Field size must be positive");
        }
        // ignore
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkOpen();
        // no-op - always escape
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkOpen();
        return (int) TimeUnit.MILLISECONDS.toSeconds(requestMeta.queryTimeoutInMs());
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkOpen();
        if (seconds < 0) {
            throw new SQLException("Query timeout must be positive");
        }
        requestMeta.queryTimeout(TimeUnit.SECONDS.toMillis(seconds));
    }

    @Override
    public void cancel() throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Cancel not supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkOpen();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        checkOpen();
        // no-op (doc is confusing - says no-op but also to throw an exception)
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkOpen();
        initResultSet(sql, Collections.emptyList());
        return true;
    }

    // execute the query and handle the rs closing and initialization
    protected void initResultSet(String sql, List<SqlTypedParamValue> params) throws SQLException {
        // close previous result set
        closeResultSet();

        Cursor cursor = con.client.query(sql, params, requestMeta);
        rs = new JdbcResultSet(cfg, this, cursor);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkOpen();
        return rs;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        long count = getLargeUpdateCount();
        return count > Integer.MAX_VALUE ? Integer.MAX_VALUE : count < Integer.MIN_VALUE ? Integer.MIN_VALUE : (int) count;
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        checkOpen();
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkOpen();
        closeResultSet();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        if (ResultSet.FETCH_REVERSE != direction
                || ResultSet.FETCH_FORWARD != direction
                || ResultSet.FETCH_UNKNOWN != direction) {
            throw new SQLException("Invalid direction specified");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkOpen();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkOpen();
        if (rows < 0) {
            throw new SQLException("Fetch size must be positive");
        }
        requestMeta.fetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkOpen();
        // the spec is somewhat unclear. It looks like there are 3 states:
        // unset (in this case -1 which the user cannot set) - in this case, the default fetch size is returned
        // 0 meaning the hint is disabled (the user has called setFetch)
        // >0 means actual hint

        // tl;dr - unless the user set it, returning the default is fine
        return requestMeta.fetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkOpen();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkOpen();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }

    @Override
    public void clearBatch() throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkOpen();
        return con;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkOpen();
        if (CLOSE_CURRENT_RESULT == current) {
            closeResultSet();
            return false;
        }
        if (KEEP_CURRENT_RESULT == current || CLOSE_ALL_RESULTS == current) {
            throw new SQLException("Invalid current parameter");
        }

        throw new SQLFeatureNotSupportedException("Multiple ResultSets not supported");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Generated keys not supported");
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        checkOpen();
        throw new SQLFeatureNotSupportedException("Batching not supported");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkOpen();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkOpen();
        // no-op
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkOpen();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkOpen();
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkOpen();
        return closeOnCompletion;
    }

    protected final void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Statement is closed");
        }
    }

    protected final void closeResultSet() throws SQLException {
        if (rs != null) {
            ignoreResultSetClose = true;
            try {
                rs.close();
            } finally {
                rs = null;
                ignoreResultSetClose = false;
            }
        }
    }

    final void resultSetWasClosed() throws SQLException {
        if (closeOnCompletion && !ignoreResultSetClose) {
            close();
        }
    }
}
