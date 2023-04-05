/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class EsqlQueryRequest extends ActionRequest implements CompositeIndicesRequest {

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField COLUMNAR_FIELD = new ParseField("columnar");
    private static final ParseField TIME_ZONE_FIELD = new ParseField("time_zone");
    private static final ParseField FILTER_FIELD = new ParseField("filter");
    private static final ParseField PRAGMA_FIELD = new ParseField("pragma");

    private static final ObjectParser<EsqlQueryRequest, Void> PARSER = objectParser(EsqlQueryRequest::new);

    private String query;
    private boolean columnar;
    private ZoneId zoneId;
    private QueryBuilder filter;
    private QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);

    public EsqlQueryRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.hasText(query) == false) {
            validationException = addValidationError("[query] is required", validationException);
        }
        if (Build.CURRENT.isSnapshot() == false && pragmas.isEmpty() == false) {
            validationException = addValidationError("[pragma] only allowed in snapshot builds", validationException);
        }
        return validationException;
    }

    public EsqlQueryRequest() {}

    public void query(String query) {
        this.query = query;
    }

    public String query() {
        return query;
    }

    public void columnar(boolean columnar) {
        this.columnar = columnar;
    }

    public boolean columnar() {
        return columnar;
    }

    public void zoneId(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    public void filter(QueryBuilder filter) {
        this.filter = filter;
    }

    public QueryBuilder filter() {
        return filter;
    }

    public void pragmas(QueryPragmas pragmas) {
        this.pragmas = pragmas;
    }

    public QueryPragmas pragmas() {
        return pragmas;
    }

    public static EsqlQueryRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private static ObjectParser<EsqlQueryRequest, Void> objectParser(Supplier<EsqlQueryRequest> supplier) {
        ObjectParser<EsqlQueryRequest, Void> parser = new ObjectParser<>("esql/query", false, supplier);
        parser.declareString(EsqlQueryRequest::query, QUERY_FIELD);
        parser.declareBoolean(EsqlQueryRequest::columnar, COLUMNAR_FIELD);
        parser.declareString((request, zoneId) -> request.zoneId(ZoneId.of(zoneId)), TIME_ZONE_FIELD);
        parser.declareObject(EsqlQueryRequest::filter, (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p), FILTER_FIELD);
        parser.declareObject(
            EsqlQueryRequest::pragmas,
            (p, c) -> new QueryPragmas(Settings.builder().loadFromMap(p.map()).build()),
            PRAGMA_FIELD
        );
        return parser;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }
}
