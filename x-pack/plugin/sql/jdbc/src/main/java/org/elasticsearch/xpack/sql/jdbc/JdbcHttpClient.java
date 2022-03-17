/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.xpack.sql.client.ClientException;
import org.elasticsearch.xpack.sql.client.ClientVersion;
import org.elasticsearch.xpack.sql.client.HttpClient;
import org.elasticsearch.xpack.sql.client.HttpClient.ResponseWithWarnings;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.MainResponse;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlQueryRequest;
import org.elasticsearch.xpack.sql.proto.SqlQueryResponse;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.proto.core.TimeValue;
import org.elasticsearch.xpack.sql.proto.core.Tuple;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.sql.client.StringUtils.EMPTY;

/**
 * JDBC specific HTTP client.
 * Since JDBC is not thread-safe, neither is this class.
 */
class JdbcHttpClient {
    private final HttpClient httpClient;
    private final JdbcConnection jdbcConn;
    private final JdbcConfiguration conCfg;
    private InfoResponse serverInfo;

    /**
     * The SQLException is the only type of Exception the JDBC API can throw (and that the user expects).
     * If we remove it, we need to make sure no other types of Exceptions (runtime or otherwise) are thrown
     */
    JdbcHttpClient(JdbcConnection jdbcConn) throws SQLException {
        this(jdbcConn, true);
    }

    JdbcHttpClient(JdbcConnection jdbcConn, boolean checkServer) throws SQLException {
        this.jdbcConn = jdbcConn;
        conCfg = jdbcConn.config();
        httpClient = new HttpClient(conCfg);
        if (checkServer) {
            this.serverInfo = fetchServerInfo();
            checkServerVersion();
        }
    }

    boolean ping(long timeoutInMs) throws SQLException {
        return httpClient.ping(timeoutInMs);
    }

    Cursor query(String sql, List<SqlTypedParamValue> params, RequestMeta meta) throws SQLException {
        int fetch = meta.fetchSize() > 0 ? meta.fetchSize() : conCfg.pageSize();
        SqlQueryRequest sqlRequest = new SqlQueryRequest(
            sql,
            params,
            conCfg.zoneId(),
            jdbcConn.getCatalog(),
            fetch,
            TimeValue.timeValueMillis(meta.queryTimeoutInMs()),
            TimeValue.timeValueMillis(meta.pageTimeoutInMs()),
            Boolean.FALSE,
            null,
            new RequestInfo(Mode.JDBC, ClientVersion.CURRENT),
            conCfg.fieldMultiValueLeniency(),
            conCfg.indexIncludeFrozen(),
            conCfg.binaryCommunication()
        );
        ResponseWithWarnings<SqlQueryResponse> response = httpClient.query(sqlRequest);
        return new DefaultCursor(
            this,
            response.response().cursor(),
            toJdbcColumnInfo(response.response().columns()),
            response.response().rows(),
            meta,
            response.warnings()
        );
    }

    /**
     * Read the next page of results and returning
     * the scroll id to use to fetch the next page.
     */
    Tuple<String, List<List<Object>>> nextPage(String cursor, RequestMeta meta) throws SQLException {
        SqlQueryRequest sqlRequest = new SqlQueryRequest(
            cursor,
            TimeValue.timeValueMillis(meta.queryTimeoutInMs()),
            TimeValue.timeValueMillis(meta.pageTimeoutInMs()),
            new RequestInfo(Mode.JDBC),
            conCfg.binaryCommunication()
        );
        SqlQueryResponse response = httpClient.query(sqlRequest).response();
        return new Tuple<>(response.cursor(), response.rows());
    }

    boolean queryClose(String cursor) throws SQLException {
        return httpClient.queryClose(cursor, Mode.JDBC);
    }

    InfoResponse serverInfo() throws SQLException {
        if (serverInfo == null) {
            serverInfo = fetchServerInfo();
        }
        return serverInfo;
    }

    private InfoResponse fetchServerInfo() throws SQLException {
        try {
            MainResponse mainResponse = httpClient.serverInfo();
            SqlVersion version = SqlVersion.fromString(mainResponse.getVersion());
            return new InfoResponse(mainResponse.getClusterName(), version);
        } catch (ClientException ex) {
            throw new SQLException(ex);
        }
    }

    private void checkServerVersion() throws SQLException {
        if (ClientVersion.isServerCompatible(serverInfo.version) == false) {
            throw new SQLException(
                "This version of the JDBC driver is only compatible with Elasticsearch version "
                    + ClientVersion.CURRENT.majorMinorToString()
                    + " or newer; attempting to connect to a server version "
                    + serverInfo.version.toString()
            );
        }
    }

    /**
     * Converts REST column metadata into JDBC column metadata
     */
    private List<JdbcColumnInfo> toJdbcColumnInfo(List<ColumnInfo> columns) throws SQLException {
        List<JdbcColumnInfo> cols = new ArrayList<>(columns.size());
        for (ColumnInfo info : columns) {
            cols.add(new JdbcColumnInfo(info.name(), TypeUtils.of(info.esType()), EMPTY, EMPTY, EMPTY, EMPTY, info.displaySize()));
        }
        return cols;
    }
}
