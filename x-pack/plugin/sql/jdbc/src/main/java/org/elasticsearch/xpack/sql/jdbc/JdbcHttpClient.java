/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.sql.client.HttpClient;
import org.elasticsearch.xpack.sql.client.Version;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.MainResponse;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.RequestInfo;
import org.elasticsearch.xpack.sql.proto.SqlQueryRequest;
import org.elasticsearch.xpack.sql.proto.SqlQueryResponse;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

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
    private final JdbcConfiguration conCfg;
    private InfoResponse serverInfo;

    /**
     * The SQLException is the only type of Exception the JDBC API can throw (and that the user expects).
     * If we remove it, we need to make sure no other types of Exceptions (runtime or otherwise) are thrown
     */
    JdbcHttpClient(JdbcConfiguration conCfg) throws SQLException {
        this(conCfg, true);
    }

    JdbcHttpClient(JdbcConfiguration conCfg, boolean checkServer) throws SQLException {
        httpClient = new HttpClient(conCfg);
        this.conCfg = conCfg;
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
        SqlQueryRequest sqlRequest = new SqlQueryRequest(sql, params, conCfg.zoneId(),
                fetch,
                TimeValue.timeValueMillis(meta.timeoutInMs()),
                TimeValue.timeValueMillis(meta.queryTimeoutInMs()),
                null,
                Boolean.FALSE,
                null,
                new RequestInfo(Mode.JDBC),
                conCfg.fieldMultiValueLeniency(),
                conCfg.indexIncludeFrozen(),
                conCfg.binaryCommunication());
        SqlQueryResponse response = httpClient.query(sqlRequest);
        return new DefaultCursor(this, response.cursor(), toJdbcColumnInfo(response.columns()), response.rows(), meta);
    }

    /**
     * Read the next page of results and returning
     * the scroll id to use to fetch the next page.
     */
    Tuple<String, List<List<Object>>> nextPage(String cursor, RequestMeta meta) throws SQLException {
        SqlQueryRequest sqlRequest = new SqlQueryRequest(cursor, TimeValue.timeValueMillis(meta.timeoutInMs()),
                TimeValue.timeValueMillis(meta.queryTimeoutInMs()), new RequestInfo(Mode.JDBC), conCfg.binaryCommunication());
        SqlQueryResponse response = httpClient.query(sqlRequest);
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
        MainResponse mainResponse = httpClient.serverInfo();
        Version version = Version.fromString(mainResponse.getVersion());
        return new InfoResponse(mainResponse.getClusterName(), version.major, version.minor, version.revision);
    }
    
    private void checkServerVersion() throws SQLException {
        if (serverInfo.majorVersion != Version.CURRENT.major
                || serverInfo.minorVersion != Version.CURRENT.minor
                || serverInfo.revisionVersion != Version.CURRENT.revision) {
            throw new SQLException("This version of the JDBC driver is only compatible with Elasticsearch version " +
                    Version.CURRENT.toString() + ", attempting to connect to a server version " + serverInfo.versionString());
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
