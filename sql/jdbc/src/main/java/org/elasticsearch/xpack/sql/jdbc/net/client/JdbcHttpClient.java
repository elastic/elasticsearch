/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.client;

import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcConfiguration;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcException;
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
import org.elasticsearch.xpack.sql.jdbc.net.protocol.TimeoutInfo;
import org.elasticsearch.xpack.sql.jdbc.util.BytesArray;
import org.elasticsearch.xpack.sql.jdbc.util.FastByteArrayInputStream;
import org.elasticsearch.xpack.sql.net.client.util.StringUtils;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

public class JdbcHttpClient implements Closeable {
    @FunctionalInterface
    interface DataInputFunction<R> {
        R apply(DataInput in) throws IOException, SQLException;
    }

    private final HttpClient http;
    private final JdbcConfiguration conCfg;
    private InfoResponse serverInfo;

    public JdbcHttpClient(JdbcConfiguration conCfg) {
        http = new HttpClient(conCfg);
        this.conCfg = conCfg;
    }

    public boolean ping(long timeoutInMs) {
        long oldTimeout = http.getNetworkTimeout();
        // NOCOMMIT this seems race condition-y
        http.setNetworkTimeout(timeoutInMs);
        try {
            return http.head(StringUtils.EMPTY);
        } finally {
            http.setNetworkTimeout(oldTimeout);
        }
    }

    public Cursor query(String sql, RequestMeta meta) throws SQLException {
        int fetch = meta.fetchSize() >= 0 ? meta.fetchSize() : conCfg.pageSize();
        QueryInitRequest request = new QueryInitRequest(sql, fetch, conCfg.timeZone(), timeout(meta));
        BytesArray ba = http.put(out -> Proto.INSTANCE.writeRequest(request, out));
        QueryInitResponse response = doIO(ba, in -> (QueryInitResponse) readResponse(request, in));
        return new DefaultCursor(this, response.requestId, (Page) response.data, meta);
    }

    /**
     * Read the next page of results, updating the {@link Page} and returning
     * the scroll id to use to fetch the next page.
     */
    public String nextPage(String requestId, Page page, RequestMeta meta) throws SQLException {
        QueryPageRequest request = new QueryPageRequest(requestId, timeout(meta), page);
        BytesArray ba = http.put(out -> Proto.INSTANCE.writeRequest(request, out));
        return doIO(ba, in -> ((QueryPageResponse) readResponse(request, in)).requestId);
    }

    public InfoResponse serverInfo() throws SQLException {
        if (serverInfo == null) {
            serverInfo = fetchServerInfo();
        }
        return serverInfo;
    }

    private InfoResponse fetchServerInfo() throws SQLException {
        InfoRequest request = new InfoRequest();
        BytesArray ba = http.put(out -> Proto.INSTANCE.writeRequest(request, out));
        return doIO(ba, in -> (InfoResponse) readResponse(request, in));
    }

    public List<String> metaInfoTables(String pattern) throws SQLException {
        MetaTableRequest request = new MetaTableRequest(pattern);
        BytesArray ba = http.put(out -> Proto.INSTANCE.writeRequest(request, out));
        return doIO(ba, in -> ((MetaTableResponse) readResponse(request, in)).tables);
    }

    public List<MetaColumnInfo> metaInfoColumns(String tablePattern, String columnPattern) throws SQLException {
        MetaColumnRequest request = new MetaColumnRequest(tablePattern, columnPattern);
        BytesArray ba = http.put(out -> Proto.INSTANCE.writeRequest(request, out));
        return doIO(ba, in -> ((MetaColumnResponse) readResponse(request, in)).columns);
    }

    public void close() {
        http.close();
    }

    public void setNetworkTimeout(long millis) {
        http.setNetworkTimeout(millis);
    }

    public long getNetworkTimeout() {
        return http.getNetworkTimeout();
    }

    private static <T> T doIO(BytesArray ba, DataInputFunction<T> action) throws SQLException {
        try (DataInputStream in = new DataInputStream(new FastByteArrayInputStream(ba))) {
            return action.apply(in);
        } catch (IOException ex) {
            throw new JdbcException(ex, "Cannot read response");
        }
    }

    private static Response readResponse(Request request, DataInput in) throws IOException, SQLException {
        Response response = Proto.INSTANCE.readResponse(request, in);

        if (response.responseType() == ResponseType.EXCEPTION) {
            ExceptionResponse ex = (ExceptionResponse) response;
            throw ex.asException();
        }
        if (response.responseType() == ResponseType.EXCEPTION) {
            ErrorResponse error = (ErrorResponse) response;
            throw new JdbcException("%s", error.stack);
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