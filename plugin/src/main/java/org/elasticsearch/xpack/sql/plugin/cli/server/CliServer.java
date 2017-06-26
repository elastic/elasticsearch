/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.cli.server;

import org.elasticsearch.Build;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.SqlException;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ErrorResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.Proto.Action;
import org.elasticsearch.xpack.sql.cli.net.protocol.Request;
import org.elasticsearch.xpack.sql.cli.net.protocol.Response;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.execution.search.SearchHitRowSetCursor;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryPageRequest;
import org.elasticsearch.xpack.sql.net.client.util.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.function.Supplier;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.sql.net.client.util.StringUtils.EMPTY;

public class CliServer {

    private final PlanExecutor executor;
    private final Supplier<InfoResponse> infoResponse;

    public CliServer(PlanExecutor executor, String clusterName, Supplier<String> nodeName, Version version, Build build) {
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
            else if (req instanceof CommandRequest) {
                command((CommandRequest) req, listener);
            }
        } catch (Exception ex) {
            listener.onResponse(exception(ex, req.action));
        }
    }

    public InfoResponse info(InfoRequest req) {
        return infoResponse.get();
    }

    public void command(CommandRequest req, ActionListener<Response> listener) {
        final long start = System.currentTimeMillis();
        
        executor.sql(req.command, wrap(
                c -> {
                    long stop = System.currentTimeMillis();
                    String requestId = EMPTY;
                    if (c.hasNextSet() && c instanceof SearchHitRowSetCursor) {
                        requestId = StringUtils.nullAsEmpty(((SearchHitRowSetCursor) c).scrollId());
                    }

                    listener.onResponse(new CommandResponse(start, stop, requestId, c));
                }, 
                ex -> exception(ex, req.action)));
    }

    public void queryPage(QueryPageRequest req, ActionListener<Response> listener) {
        throw new UnsupportedOperationException();
    }

    private static Response exception(Throwable cause, Action action) {
        String message = EMPTY;
        String cs = EMPTY;
        if (cause != null) {
            if (StringUtils.hasText(cause.getMessage())) {
                message = cause.getMessage();
            }
            cs = cause.getClass().getName();
        }

        if (expectedException(cause)) {
            return new ExceptionResponse(action, message, cs);
        }
        else {
            // TODO: might want to 'massage' this
            StringWriter sw = new StringWriter();
            cause.printStackTrace(new PrintWriter(sw));
            return new ErrorResponse(action, message, cs, sw.toString());
        }
    }

    private static boolean expectedException(Throwable cause) {
        return (cause instanceof SqlException || cause instanceof ResourceNotFoundException);
    }
}