/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.cli.server;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.CommandResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.ExceptionResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoRequest;
import org.elasticsearch.xpack.sql.cli.net.protocol.InfoResponse;
import org.elasticsearch.xpack.sql.cli.net.protocol.Request;
import org.elasticsearch.xpack.sql.cli.net.protocol.Response;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.execution.search.SearchHitRowSetCursor;
import org.elasticsearch.xpack.sql.jdbc.net.protocol.QueryPageRequest;
import org.elasticsearch.xpack.sql.plugin.cli.http.CliServerProtoUtils;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.TimeZone;
import java.util.function.Supplier;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.sql.util.StringUtils.EMPTY;

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
            else {
                listener.onResponse(new ExceptionResponse(req.action, "Invalid requested", null));
            }
        } catch (Exception ex) {
            listener.onResponse(CliServerProtoUtils.exception(ex, req.action));
        }
    }

    public InfoResponse info(InfoRequest req) {
        return infoResponse.get();
    }

    public void command(CommandRequest req, ActionListener<Response> listener) {
        final long start = System.currentTimeMillis();

        // TODO support non-utc for cli server
        executor.sql(req.command, TimeZone.getTimeZone("UTC"), wrap(
                c -> {
                    long stop = System.currentTimeMillis();
                    String requestId = EMPTY;
                    if (c.hasNextSet() && c instanceof SearchHitRowSetCursor) {
                        requestId = StringUtils.nullAsEmpty(((SearchHitRowSetCursor) c).scrollId());
                    }

                    listener.onResponse(new CommandResponse(start, stop, requestId, c));
                }, 
                ex -> listener.onResponse(CliServerProtoUtils.exception(ex, req.action))));
    }

    public void queryPage(QueryPageRequest req, ActionListener<Response> listener) {
        throw new UnsupportedOperationException();
    }

}