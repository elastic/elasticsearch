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
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.SqlExceptionType;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ProtoUtils;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryInitRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryInitResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryPageRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryPageResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Response;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.TimeoutInfo;
import org.elasticsearch.xpack.sql.jdbc.util.BytesArray;
import org.elasticsearch.xpack.sql.jdbc.util.FastByteArrayInputStream;
import org.elasticsearch.xpack.sql.net.client.util.StringUtils;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
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
        BytesArray ba = http.put(out -> queryRequest(out, meta, sql));
        return doIO(ba, in -> queryResponse(in, meta));
    }

    private void queryRequest(DataOutput out, RequestMeta meta, String sql) throws IOException {
        int fetch = meta.fetchSize() >= 0 ? meta.fetchSize() : conCfg.pageSize();
        ProtoUtils.write(out, new QueryInitRequest(fetch, sql, timeout(meta)));
    }

    public String nextPage(String requestId, Page page, RequestMeta meta) throws SQLException {
        BytesArray ba = http.put(out -> ProtoUtils.write(out, new QueryPageRequest(requestId, timeout(meta))));
        return doIO(ba, in -> pageResponse(in, page));
    }

    private TimeoutInfo timeout(RequestMeta meta) {
        // client time
        long clientTime = Instant.now().toEpochMilli();

        // timeout (in ms)
        long timeout = meta.timeoutInMs();
        if (timeout == 0) {
            timeout = conCfg.getQueryTimeout();
        }
        return new TimeoutInfo(clientTime, timeout, conCfg.getPageTimeout());
    }

    private Cursor queryResponse(DataInput in, RequestMeta meta) throws IOException, SQLException {
        QueryInitResponse response = readResponse(in, Action.QUERY_INIT);

        // finally read data
        // allocate columns
        int rows = in.readInt();
        Page page = Page.of(response.columns, rows);
        readData(in, page, rows);

        return new DefaultCursor(this, response.requestId, page, meta);
    }

    private void readData(DataInput in, Page page, int rows) throws IOException {
        page.resize(rows);
        int[] jdbcTypes = page.columnInfo().stream()
                .mapToInt(c -> c.type)
                .toArray();
        
        for (int row = 0; row < rows; row++) {
            for (int column = 0; column < jdbcTypes.length; column++) {
                page.column(column)[row] = ProtoUtils.readValue(in, jdbcTypes[column]);
            }
        }
    }

    private String pageResponse(DataInput in, Page page) throws IOException, SQLException {
        QueryPageResponse response = readResponse(in, Action.QUERY_PAGE);

        // read the data
        // allocate columns
        int rows = in.readInt();
        page.resize(rows); // NOCOMMIT I believe this is duplicated with readData
        readData(in, page, rows);

        return response.requestId;
    }

    public InfoResponse serverInfo() throws SQLException {
        if (serverInfo == null) {
            serverInfo = fetchServerInfo();
        }
        return serverInfo;
    }

    private InfoResponse fetchServerInfo() throws SQLException {
        BytesArray ba = http.put(out -> ProtoUtils.write(out, new InfoRequest()));
        return doIO(ba, in -> readResponse(in, Action.INFO));
    }

    public List<String> metaInfoTables(String pattern) throws SQLException {
        BytesArray ba = http.put(out -> ProtoUtils.write(out, new MetaTableRequest(pattern)));

        return doIO(ba, in -> {
            MetaTableResponse res = readResponse(in, Action.META_TABLE);
            return res.tables;
        });
    }

    public List<MetaColumnInfo> metaInfoColumns(String tablePattern, String columnPattern) throws SQLException {
        BytesArray ba = http.put(out -> ProtoUtils.write(out, new MetaColumnRequest(tablePattern, columnPattern)));

        return doIO(ba, in -> {
            MetaColumnResponse res = readResponse(in, Action.META_COLUMN);
            return res.columns;
        });
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

    @SuppressWarnings("unchecked")
    private static <R extends Response> R readResponse(DataInput in, Action expected) throws IOException, SQLException {
        String errorMessage = ProtoUtils.readHeader(in);
        if (errorMessage != null) {
            throw new JdbcException(errorMessage);
        }
        
        int header = in.readInt();

        Action action = Action.from(header);
        if (expected != action) {
            throw new JdbcException("Expected response for %s, found %s", expected, action);
        }

        Response response = ProtoUtils.readResponse(in, header);

        // NOCOMMIT why not move the throw login into readResponse?
        if (response instanceof ExceptionResponse) {
            ExceptionResponse ex = (ExceptionResponse) response;
            throw SqlExceptionType.asException(ex.asSql, ex.message);
        }
        if (response instanceof ErrorResponse) {
            ErrorResponse error = (ErrorResponse) response;
            throw new JdbcException("%s", error.stack);
        }
        if (response instanceof Response) {
            // NOCOMMIT I'd feel more comfortable either returning Response and passing the class in and calling responseClass.cast(response)
            return (R) response;
        }

        throw new JdbcException("Invalid response status %08X", header);
    }
}
