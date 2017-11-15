/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.client;

import org.elasticsearch.xpack.sql.jdbc.JdbcSQLException;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConfiguration;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ErrorResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnInfo;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Page;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.ResponseType;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryInitRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryInitResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryPageRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryPageResponse;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.elasticsearch.xpack.sql.protocol.shared.TimeoutInfo;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

public class JdbcHttpClient {
    @FunctionalInterface
    interface DataInputFunction<R> {
        R apply(DataInput in) throws IOException, SQLException;
    }

    private final HttpClient http;
    private final JdbcConfiguration conCfg;
    private InfoResponse serverInfo;

    public JdbcHttpClient(JdbcConfiguration conCfg) throws SQLException {
        http = new HttpClient(conCfg);
        this.conCfg = conCfg;
    }

    public boolean ping(long timeoutInMs) throws SQLException {
        long oldTimeout = http.getNetworkTimeout();
        try {
            // this works since the connection is single-threaded and its configuration not shared
            // with others connections
        http.setNetworkTimeout(timeoutInMs);
            return http.head();
        } finally {
            http.setNetworkTimeout(oldTimeout);
        }
    }

    public Cursor query(String sql, RequestMeta meta) throws SQLException {
        int fetch = meta.fetchSize() > 0 ? meta.fetchSize() : conCfg.pageSize();
        QueryInitRequest request = new QueryInitRequest(sql, fetch, conCfg.timeZone(), timeout(meta));
        QueryInitResponse response = (QueryInitResponse) checkResponse(http.put(request));
        return new DefaultCursor(this, response.cursor(), (Page) response.data, meta);
    }

    /**
     * Read the next page of results, updating the {@link Page} and returning
     * the scroll id to use to fetch the next page.
     */
    public byte[] nextPage(byte[] cursor, Page page, RequestMeta meta) throws SQLException {
        QueryPageRequest request = new QueryPageRequest(cursor, timeout(meta), page);
        return ((QueryPageResponse) checkResponse(http.put(request))).cursor();
    }

    public InfoResponse serverInfo() throws SQLException {
        if (serverInfo == null) {
            serverInfo = fetchServerInfo();
        }
        return serverInfo;
    }

    private InfoResponse fetchServerInfo() throws SQLException {
        InfoRequest request = new InfoRequest();
        return (InfoResponse) checkResponse(http.put(request));
    }

    public List<String> metaInfoTables(String pattern) throws SQLException {
        MetaTableRequest request = new MetaTableRequest(pattern);
        return ((MetaTableResponse) checkResponse(http.put(request))).tables;
    }

    public List<MetaColumnInfo> metaInfoColumns(String tablePattern, String columnPattern) throws SQLException {
        MetaColumnRequest request = new MetaColumnRequest(tablePattern, columnPattern);
        return ((MetaColumnResponse) checkResponse(http.put(request))).columns;
    }

    public void setNetworkTimeout(long millis) {
        http.setNetworkTimeout(millis);
    }

    public long getNetworkTimeout() {
        return http.getNetworkTimeout();
    }

    private static Response checkResponse(Response response) throws SQLException {
        if (response.responseType() == ResponseType.EXCEPTION) {
            ExceptionResponse ex = (ExceptionResponse) response;
            throw ex.asException();
        }
        if (response.responseType() == ResponseType.ERROR) {
            ErrorResponse error = (ErrorResponse) response;
            // TODO: this could be made configurable to switch between message to error
            throw new JdbcSQLException("Server returned error: [" + error.stack + "]");
        }
        return response;
    }

    private TimeoutInfo timeout(RequestMeta meta) {
        // client time
        long clientTime = Instant.now().toEpochMilli();

        // timeout (in ms)
        long timeout = meta.timeoutInMs();
        if (timeout == 0) {
            timeout = conCfg.queryTimeout();
        }
        return new TimeoutInfo(clientTime, timeout, conCfg.pageTimeout());
    }
}