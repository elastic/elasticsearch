/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.main.MainAction;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.xpack.sql.analysis.catalog.EsIndex;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfo;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ErrorResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnInfo;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryInitRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryInitResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryPageRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryPageResponse;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlRequest;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponse;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.SqlExceptionType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.sql.util.StringUtils.EMPTY;

public class RestSqlJdbcAction extends AbstractSqlProtocolRestAction {
    private final SqlLicenseChecker sqlLicenseChecker;

    public RestSqlJdbcAction(Settings settings, RestController controller, SqlLicenseChecker sqlLicenseChecker) {
        super(settings, Proto.INSTANCE);
        controller.registerHandler(POST, "/_sql/jdbc", this);
        this.sqlLicenseChecker = sqlLicenseChecker;
    }

    @Override
    public String getName() {
        return "xpack_sql_jdbc_action";
    }

    @Override
    protected RestChannelConsumer innerPrepareRequest(Request request, Client client)
            throws IOException {
        Consumer<RestChannel> consumer = operation(request, client);
        return consumer::accept;
    }

    @Override
    protected ErrorResponse buildErrorResponse(Request request, String message, String cause, String stack) {
        return new ErrorResponse((RequestType) request.requestType(), message, cause, stack);
    }

    @Override
    protected ExceptionResponse buildExceptionResponse(Request request, String message, String cause, SqlExceptionType exceptionType) {
        return new ExceptionResponse((RequestType) request.requestType(), message, cause, exceptionType);
    }

    /**
     * Actual implementation of the operation
     */
    public Consumer<RestChannel> operation(Request request, Client client) throws IOException {
        sqlLicenseChecker.checkIfJdbcAllowed();
        RequestType requestType = (RequestType) request.requestType();
        switch (requestType) {
        case INFO:
            return channel -> client.execute(MainAction.INSTANCE, new MainRequest(), toActionListener(request, channel, response ->
                    new InfoResponse(response.getNodeName(), response.getClusterName().value(),
                            response.getVersion().major, response.getVersion().minor, response.getVersion().toString(),
                            response.getBuild().shortHash(), response.getBuild().date())));
        case META_TABLE:
            return metaTable(client, (MetaTableRequest) request);
        case META_COLUMN:
            return metaColumn(client, (MetaColumnRequest) request);
        case QUERY_INIT:
            return queryInit(client, (QueryInitRequest) request);
        case QUERY_PAGE:
            return queryPage(client, (QueryPageRequest) request);
        default:
            throw new IllegalArgumentException("Unsupported action [" + requestType + "]");
        }
    }

    private Consumer<RestChannel> metaTable(Client client, MetaTableRequest request) {
        String indexPattern = hasText(request.pattern()) ? StringUtils.jdbcToEsPattern(request.pattern()) : "*";
        SqlGetIndicesAction.Request getRequest = new SqlGetIndicesAction.Request(IndicesOptions.lenientExpandOpen(), indexPattern);
        getRequest.local(true); // TODO serialization not supported by get indices action
        return channel -> client.execute(SqlGetIndicesAction.INSTANCE, getRequest, toActionListener(request, channel, response -> {
            return new MetaTableResponse(response.indices().stream()
                    .map(EsIndex::name)
                    .collect(toList()));
        }));
    }

    private Consumer<RestChannel> metaColumn(Client client, MetaColumnRequest request) {
        String indexPattern = Strings.hasText(request.tablePattern()) ? StringUtils.jdbcToEsPattern(request.tablePattern()) : "*";
        Pattern columnMatcher = hasText(request.columnPattern()) ? StringUtils.likeRegex(request.columnPattern()) : null;

        SqlGetIndicesAction.Request getRequest = new SqlGetIndicesAction.Request(IndicesOptions.lenientExpandOpen(), indexPattern);
        getRequest.local(true); // TODO serialization not supported by get indices action
        return channel -> client.execute(SqlGetIndicesAction.INSTANCE, getRequest, toActionListener(request, channel, response -> {
            List<MetaColumnInfo> columns = new ArrayList<>();
            for (EsIndex esIndex : response.indices()) {
              int pos = 0;
              for (Map.Entry<String, DataType> entry : esIndex.mapping().entrySet()) {
                  pos++;
                  String name = entry.getKey();
                  if (columnMatcher == null || columnMatcher.matcher(name).matches()) {
                      DataType type = entry.getValue();
                      // the column size it's actually its precision (based on the Javadocs) 
                      columns.add(new MetaColumnInfo(esIndex.name(), name, type.sqlType(), type.precision(), pos));
                  }
              }
            }
            return new MetaColumnResponse(columns);
        }));
    }

    private Consumer<RestChannel> queryInit(Client client, QueryInitRequest request) {
        SqlRequest sqlRequest = new SqlRequest(request.query, SqlRequest.DEFAULT_TIME_ZONE, request.fetchSize, Cursor.EMPTY);
        sqlRequest.timeZone(DateTimeZone.forTimeZone(request.timeZone));
        long start = System.nanoTime();
        return channel -> client.execute(SqlAction.INSTANCE, sqlRequest, toActionListener(request, channel, response -> {
            List<JDBCType> types = new ArrayList<>(response.columns().size());
            List<ColumnInfo> columns = new ArrayList<>(response.columns().size());
            for (SqlResponse.ColumnInfo info : response.columns()) {
                types.add(info.jdbcType());
                columns.add(new ColumnInfo(info.name(), info.jdbcType(), EMPTY, EMPTY, EMPTY, EMPTY, info.displaySize()));
            }
            return new QueryInitResponse(System.nanoTime() - start, serializeCursor(response.cursor(), types), columns,
                    new SqlResponsePayload(types, response.rows()));
        }));
    }

    private Consumer<RestChannel> queryPage(Client client, QueryPageRequest request) {
        Cursor cursor;
        List<JDBCType> types;
        try (StreamInput in = new NamedWriteableAwareStreamInput(new BytesArray(request.cursor).streamInput(), CURSOR_REGISTRY)) {
            cursor = in.readNamedWriteable(Cursor.class);
            types = in.readList(r -> JDBCType.valueOf(r.readVInt()));
        } catch (IOException e) {
            throw new IllegalArgumentException("error reading the cursor");
        }
        SqlRequest sqlRequest = new SqlRequest(EMPTY, SqlRequest.DEFAULT_TIME_ZONE, 0, cursor);
        long start = System.nanoTime();
        return channel -> client.execute(SqlAction.INSTANCE, sqlRequest, toActionListener(request, channel, response -> {
            return new QueryPageResponse(System.nanoTime() - start, serializeCursor(response.cursor(), types),
                    new SqlResponsePayload(types, response.rows()));
        }));
    }

    private static byte[] serializeCursor(Cursor cursor, List<JDBCType> types) {
        if (cursor == Cursor.EMPTY) {
            return new byte[0];
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(cursor);
            out.writeVInt(types.size());
            for (JDBCType type : types) {
                out.writeVInt(type.getVendorTypeNumber());
            }
            return BytesRef.deepCopyOf(out.bytes().toBytesRef()).bytes;
        } catch (IOException e) {
            throw new RuntimeException("unexpected trouble building the cursor", e);
        }
    }
}