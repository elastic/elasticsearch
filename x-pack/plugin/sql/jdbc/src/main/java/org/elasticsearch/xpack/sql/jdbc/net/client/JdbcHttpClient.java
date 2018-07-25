/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.client;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.sql.client.HttpClient;
import org.elasticsearch.xpack.sql.client.Version;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConfiguration;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfo;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.Protocol;
import org.elasticsearch.xpack.sql.proto.SqlQueryRequest;
import org.elasticsearch.xpack.sql.proto.MainResponse;
import org.elasticsearch.xpack.sql.proto.SqlQueryResponse;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.sql.client.StringUtils.EMPTY;

/**
 * JDBC specific HTTP client.
 * Since JDBC is not thread-safe, neither is this class.
 */
public class JdbcHttpClient {
    private final HttpClient httpClient;
    private final JdbcConfiguration conCfg;
    private InfoResponse serverInfo;

    /**
     * The SQLException is the only type of Exception the JDBC API can throw (and that the user expects).
     * If we remove it, we need to make sure no other types of Exceptions (runtime or otherwise) are thrown
     */
    public JdbcHttpClient(JdbcConfiguration conCfg) throws SQLException {
        httpClient = new HttpClient(conCfg);
        this.conCfg = conCfg;
    }

    public boolean ping(long timeoutInMs) throws SQLException {
        return httpClient.ping(timeoutInMs);
    }

    public Cursor query(String sql, List<SqlTypedParamValue> params, RequestMeta meta) throws SQLException {
        int fetch = meta.fetchSize() > 0 ? meta.fetchSize() : conCfg.pageSize();
                SqlQueryRequest sqlRequest = new SqlQueryRequest(Mode.JDBC, sql, params, null,
                Protocol.TIME_ZONE,
                fetch, TimeValue.timeValueMillis(meta.timeoutInMs()), TimeValue.timeValueMillis(meta.queryTimeoutInMs()));
        SqlQueryResponse response = httpClient.query(sqlRequest);
        return new DefaultCursor(this, response.cursor(), toJdbcColumnInfo(response.columns()), response.rows(), meta);
    }

    /**
     * Read the next page of results and returning
     * the scroll id to use to fetch the next page.
     */
    public Tuple<String, List<List<Object>>> nextPage(String cursor, RequestMeta meta) throws SQLException {
        SqlQueryRequest sqlRequest = new SqlQueryRequest(Mode.JDBC, cursor, TimeValue.timeValueMillis(meta.timeoutInMs()),
            TimeValue.timeValueMillis(meta.queryTimeoutInMs()));
        SqlQueryResponse response = httpClient.query(sqlRequest);
        return new Tuple<>(response.cursor(), response.rows());
    }

    public boolean queryClose(String cursor) throws SQLException {
        return httpClient.queryClose(cursor);
    }

    public InfoResponse serverInfo() throws SQLException {
        if (serverInfo == null) {
            serverInfo = fetchServerInfo();
        }
        return serverInfo;
    }

    private InfoResponse fetchServerInfo() throws SQLException {
        MainResponse mainResponse = httpClient.serverInfo();
        Version version = Version.fromString(mainResponse.getVersion());
        return new InfoResponse(mainResponse.getClusterName(), version.major, version.minor);
    }

    /**
     * Converts REST column metadata into JDBC column metadata
     */
    private List<ColumnInfo> toJdbcColumnInfo(List<org.elasticsearch.xpack.sql.proto.ColumnInfo> columns) {
        return columns.stream().map(columnInfo ->
                new ColumnInfo(columnInfo.name(), columnInfo.jdbcType(), EMPTY, EMPTY, EMPTY, EMPTY, columnInfo.displaySize())
        ).collect(Collectors.toList());
    }
}
