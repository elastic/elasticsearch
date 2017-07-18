/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.cli;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ErrorResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.RequestType;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.execution.search.SearchHitRowSetCursor;
import org.elasticsearch.xpack.sql.plugin.AbstractSqlServer;
import org.elasticsearch.xpack.sql.protocol.shared.AbstractProto.SqlExceptionType;
import org.elasticsearch.xpack.sql.protocol.shared.Request;
import org.elasticsearch.xpack.sql.protocol.shared.Response;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.TimeZone;
import java.util.function.Supplier;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.sql.util.StringUtils.EMPTY;

public class CliServer extends AbstractSqlServer {

    private final PlanExecutor executor;
    private final Supplier<InfoResponse> infoResponse;

    public CliServer(PlanExecutor executor, String clusterName, Supplier<String> nodeName, Version version, Build build) {
        this.executor = executor;
        // Delay building the response until runtime because the node name is not available at startup
        this.infoResponse = () -> new InfoResponse(nodeName.get(), clusterName, version.major, version.minor, version.toString(),
                build.shortHash(), build.date());
    }

    @Override
    protected void innerHandle(Request req, ActionListener<Response> listener) {
        RequestType requestType = (RequestType) req.requestType();
        try {
            switch (requestType) {
            case INFO:
                listener.onResponse(info((InfoRequest) req));
                break;
            case COMMAND:
                command((CommandRequest) req, listener);
                break;
            default:
                throw new IllegalArgumentException("Unsupported action [" + requestType + "]");
            }
        } catch (Exception ex) {
            listener.onResponse(exceptionResponse(req, ex));
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

    public void command(CommandRequest req, ActionListener<Response> listener) {
        final long start = System.currentTimeMillis(); // NOCOMMIT should be nanoTime or else clock skew will skew us

        // TODO support non-utc for cli server
        executor.sql(req.command, TimeZone.getTimeZone("UTC"), wrap(
                c -> {
                    long stop = System.currentTimeMillis();
                    String requestId = EMPTY;
                    if (c.hasNextSet() && c instanceof SearchHitRowSetCursor) {
                        requestId = StringUtils.nullAsEmpty(((SearchHitRowSetCursor) c).scrollId());
                    }

                    // NOCOMMIT it looks like this tries to buffer the entire response in memory before returning it which is going to OOM some po
                    // NOCOMMIT also this blocks the current thread while it iterates the cursor
                    listener.onResponse(new CommandResponse(start, stop, requestId, CliUtils.toString(c)));
                }, 
                ex -> listener.onResponse(exceptionResponse(req, ex))));
    }
}