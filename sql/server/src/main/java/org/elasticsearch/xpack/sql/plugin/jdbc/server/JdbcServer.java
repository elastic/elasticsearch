/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.jdbc.server;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.analysis.catalog.EsType;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.execution.search.SearchHitRowSetCursor;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.ColumnInfo;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnInfo;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaColumnResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.MetaTableResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryInitRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryInitResponse;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryPageRequest;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Request;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.Response;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.sql.util.StringUtils.EMPTY;

public class JdbcServer {

    private final PlanExecutor executor;
    private final Supplier<InfoResponse> infoResponse;

    public JdbcServer(PlanExecutor executor, String clusterName, Supplier<String> nodeName, Version version, Build build) {
        this.executor = executor;
        // Delay building the response until runtime because the node name is not available at startup
        this.infoResponse = () -> new InfoResponse(nodeName.get(), clusterName, version.major, version.minor, version.toString(),
                build.shortHash(), build.date());
    }
    
    public void handle(Request req, ActionListener<Response> listener) {
        try {
            if (req instanceof InfoRequest) {
                listener.onResponse(info((InfoRequest) req));
            }
            else if (req instanceof MetaTableRequest) {
                listener.onResponse(metaTable((MetaTableRequest) req));
            }
            else if (req instanceof MetaColumnRequest) {
                listener.onResponse(metaColumn((MetaColumnRequest) req));
            }
            else if (req instanceof QueryInitRequest) {
                queryInit((QueryInitRequest) req, listener);
            }
        } catch (Exception ex) {
            listener.onResponse(JdbcServerProtoUtils.exception(ex, req.action));
        }
    }
    

    public InfoResponse info(InfoRequest req) {
        return infoResponse.get();
    }

    public MetaTableResponse metaTable(MetaTableRequest req) {
        String indexPattern = Strings.hasText(req.index) ? StringUtils.jdbcToEsPattern(req.index) : "*";

        Collection<EsType> types = executor.catalog().listTypes(indexPattern, req.type);
        return new MetaTableResponse(types.stream()
                .map(t -> t.index() + "." + t.name())
                .collect(toList()));
    }

    public MetaColumnResponse metaColumn(MetaColumnRequest req) {
        String indexPattern = Strings.hasText(req.index) ? StringUtils.jdbcToEsPattern(req.index) : "*";

        Collection<EsType> types = executor.catalog().listTypes(indexPattern, req.type);

        Pattern columnMatcher = Strings.hasText(req.column) ? StringUtils.likeRegex(req.column) : null;

        List<MetaColumnInfo> resp = new ArrayList<>();
        for (EsType type : types) {
            int pos = 0;
            for (Entry<String, DataType> entry : type.mapping().entrySet()) {
                pos++;
                if (columnMatcher == null || columnMatcher.matcher(entry.getKey()).matches()) {
                    String name = entry.getKey();
                    String table = type.index() + "." + type.name();
                    int tp = entry.getValue().sqlType().getVendorTypeNumber().intValue();
                    int size = entry.getValue().precision();
                    resp.add(new MetaColumnInfo(name, table, tp, size, pos));
                }
            }
        }

        return new MetaColumnResponse(resp);
    }


    public void queryInit(QueryInitRequest req, ActionListener<Response> listener) {
        final long start = System.currentTimeMillis();

        executor.sql(req.query, wrap(c -> {
            long stop = System.currentTimeMillis();
            String requestId = EMPTY;
            if (c.hasNextSet() && c instanceof SearchHitRowSetCursor) {
                requestId = StringUtils.nullAsEmpty(((SearchHitRowSetCursor) c).scrollId());
            }

            List<ColumnInfo> list = c.schema().stream()
                    .map(e -> new ColumnInfo(e.name(), e.type().sqlType().getVendorTypeNumber().intValue(), EMPTY, EMPTY, EMPTY, EMPTY))
                    .collect(toList());

            listener.onResponse(new QueryInitResponse(start, stop, requestId, list, c));
        }, ex -> listener.onResponse(JdbcServerProtoUtils.exception(ex, req.action))));
    }

    public void queryPage(QueryPageRequest req, ActionListener<Response> listener) {
        throw new UnsupportedOperationException();
    }
}