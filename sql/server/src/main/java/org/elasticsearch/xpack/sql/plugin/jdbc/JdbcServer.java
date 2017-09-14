/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.jdbc;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.analysis.catalog.EsIndex;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfo;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ErrorResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnInfo;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryInitRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryInitResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryPageRequest;
import org.elasticsearch.xpack.sql.plugin.AbstractSqlServer;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.SqlExceptionType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.elasticsearch.xpack.sql.session.SqlSettings;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.xpack.sql.util.StringUtils.EMPTY;

public class JdbcServer extends AbstractSqlServer {
    private final PlanExecutor executor;
    private final Supplier<InfoResponse> infoResponse;

    public JdbcServer(PlanExecutor executor, String clusterName, Supplier<String> nodeName, Version version, Build build) {
        this.executor = executor;
        // Delay building the response until runtime because the node name is not available at startup
        this.infoResponse = () -> new InfoResponse(nodeName.get(), clusterName, version.major, version.minor, version.toString(), build.shortHash(), build.date());
    }

    @Override
    protected void innerHandle(Request req, ActionListener<Response> listener) {
        RequestType requestType = (RequestType) req.requestType();
        switch (requestType) {
        case INFO:
            listener.onResponse(info((InfoRequest) req));
            break;
        case META_TABLE:
            metaTable((MetaTableRequest) req, listener);
            break;
        case META_COLUMN:
            metaColumn((MetaColumnRequest) req, listener);
            break;
        case QUERY_INIT:
            queryInit((QueryInitRequest) req, listener);
            break;
        case QUERY_PAGE:
            queryPage((QueryPageRequest) req, listener);
            break;
        default:
            throw new IllegalArgumentException("Unsupported action [" + requestType + "]");
        }
    }

    @Override
    protected ErrorResponse buildErrorResponse(Request request, String message, String cause, String stack) {
        return new ErrorResponse((RequestType) request.requestType(), message, cause, stack);
    }

    @Override
    protected ExceptionResponse buildExceptionResponse(Request request, String message, String cause,
            SqlExceptionType exceptionType) {
        return new ExceptionResponse((RequestType) request.requestType(), message, cause, exceptionType);
    }

    public InfoResponse info(InfoRequest req) {
        return infoResponse.get();
    }

    public void metaTable(MetaTableRequest req, ActionListener<Response> listener) {
        String indexPattern = hasText(req.pattern()) ? StringUtils.jdbcToEsPattern(req.pattern()) : "*";

        executor.newSession(SqlSettings.EMPTY)
            .getIndices(new String[] {indexPattern}, IndicesOptions.lenientExpandOpen(), ActionListener.wrap(result -> {
                listener.onResponse(new MetaTableResponse(result.stream()
                        .map(EsIndex::name)
                        .collect(toList())));
            }, listener::onFailure));
    }

    public void metaColumn(MetaColumnRequest req, ActionListener<Response> listener) {
        String pattern = Strings.hasText(req.tablePattern()) ? StringUtils.jdbcToEsPattern(req.tablePattern()) : "*";
        Pattern columnMatcher = hasText(req.columnPattern()) ? StringUtils.likeRegex(req.columnPattern()) : null;

        executor.newSession(SqlSettings.EMPTY)
            .getIndices(new String[] {pattern}, IndicesOptions.lenientExpandOpen(), ActionListener.wrap(result -> {
                List<MetaColumnInfo> resp = new ArrayList<>();
                for (EsIndex esIndex : result) {
                    int pos = 0;
                    for (Entry<String, DataType> entry : esIndex.mapping().entrySet()) {
                        pos++;
                        if (columnMatcher == null || columnMatcher.matcher(entry.getKey()).matches()) {
                            String name = entry.getKey();
                            String table = esIndex.name();
                            JDBCType tp = entry.getValue().sqlType();
                            int size = entry.getValue().precision();
                            resp.add(new MetaColumnInfo(table, name, tp, size, pos));
                        }
                    }
                }
                listener.onResponse(new MetaColumnResponse(resp));
            }, listener::onFailure));
    }


    public void queryInit(QueryInitRequest req, ActionListener<Response> listener) {
        final long start = System.nanoTime();

        SqlSettings sqlCfg = new SqlSettings(Settings.builder()
                .put(SqlSettings.PAGE_SIZE, req.fetchSize)
                .put(SqlSettings.TIMEZONE_ID, req.timeZone.getID())
                .build()
        );
        
        //NOCOMMIT: this should be pushed down to the TransportSqlAction to hook up pagination
        executor.sql(sqlCfg, req.query, wrap(c -> {
            long stop = System.nanoTime();

            List<ColumnInfo> columnInfo = c.schema().stream()
                    .map(e -> new ColumnInfo(e.name(), e.type().sqlType(), EMPTY, EMPTY, EMPTY, EMPTY))
                    .collect(toList());

            // NOCOMMIT paging for jdbc
            listener.onResponse(new QueryInitResponse(stop - start, new byte[0], columnInfo, new RowSetPayload(c)));
        }, ex -> listener.onResponse(exceptionResponse(req, ex))));
    }

    public void queryPage(QueryPageRequest req, ActionListener<Response> listener) {
        throw new UnsupportedOperationException();
    }
}