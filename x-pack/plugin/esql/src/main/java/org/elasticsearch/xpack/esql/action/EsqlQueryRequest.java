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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.parser.ContentLocation;
import org.elasticsearch.xpack.esql.parser.TypedParamValue;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ObjectParser.ValueType.VALUE_ARRAY;

public class EsqlQueryRequest extends ActionRequest implements CompositeIndicesRequest {

    private static final ConstructingObjectParser<TypedParamValue, Void> PARAM_PARSER = new ConstructingObjectParser<>(
        "params",
        true,
        objects -> new TypedParamValue((String) objects[1], objects[0])
    );

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField COLUMNAR_FIELD = new ParseField("columnar");
    private static final ParseField TIME_ZONE_FIELD = new ParseField("time_zone");
    private static final ParseField FILTER_FIELD = new ParseField("filter");
    private static final ParseField PRAGMA_FIELD = new ParseField("pragma");
    private static final ParseField PARAMS_FIELD = new ParseField("params");

    private static final ObjectParser<EsqlQueryRequest, Void> PARSER = objectParser(EsqlQueryRequest::new);

    private String query;
    private boolean columnar;
    private ZoneId zoneId;
    private QueryBuilder filter;
    private QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);
    private List<TypedParamValue> params = List.of();

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

    public List<TypedParamValue> params() {
        return params;
    }

    public void params(List<TypedParamValue> params) {
        this.params = params;
    }

    public static EsqlQueryRequest fromXContent(XContentParser parser) {
        EsqlQueryRequest result = PARSER.apply(parser, null);
        validateParams(result.params);
        return result;
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
        parser.declareField(EsqlQueryRequest::params, EsqlQueryRequest::parseParams, PARAMS_FIELD, VALUE_ARRAY);
        return parser;
    }

    private static List<TypedParamValue> parseParams(XContentParser p) throws IOException {
        List<TypedParamValue> result = new ArrayList<>();
        XContentParser.Token token = p.currentToken();

        if (token == XContentParser.Token.START_ARRAY) {
            Object value = null;
            String type = null;
            TypedParamValue previousParam = null;
            TypedParamValue currentParam = null;

            while ((token = p.nextToken()) != XContentParser.Token.END_ARRAY) {
                XContentLocation loc = p.getTokenLocation();

                if (token == XContentParser.Token.START_OBJECT) {
                    // we are at the start of a value/type pair... hopefully
                    currentParam = PARAM_PARSER.apply(p, null);
                    /*
                     * Always set the xcontentlocation for the first param just in case the first one happens to not meet the parsing rules
                     * that are checked later in validateParams method.
                     * Also, set the xcontentlocation of the param that is different from the previous param in list when it comes to
                     * its type being explicitly set or inferred.
                     */
                    if ((previousParam != null && previousParam.hasExplicitType() == false) || result.isEmpty()) {
                        currentParam.tokenLocation(toProto(loc));
                    }
                } else {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        value = p.text();
                        type = "keyword";
                    } else if (token == XContentParser.Token.VALUE_NUMBER) {
                        XContentParser.NumberType numberType = p.numberType();
                        if (numberType == XContentParser.NumberType.INT) {
                            value = p.intValue();
                            type = "integer";
                        } else if (numberType == XContentParser.NumberType.LONG) {
                            value = p.longValue();
                            type = "long";
                        } else if (numberType == XContentParser.NumberType.FLOAT) {
                            value = p.floatValue();
                            type = "float";
                        } else if (numberType == XContentParser.NumberType.DOUBLE) {
                            value = p.doubleValue();
                            type = "double";
                        }
                    } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        value = p.booleanValue();
                        type = "boolean";
                    } else if (token == XContentParser.Token.VALUE_NULL) {
                        value = null;
                        type = "null";
                    } else {
                        throw new XContentParseException(loc, "Failed to parse object: unexpected token [" + token + "] found");
                    }

                    currentParam = new TypedParamValue(type, value, false);
                    if ((previousParam != null && previousParam.hasExplicitType()) || result.isEmpty()) {
                        currentParam.tokenLocation(toProto(loc));
                    }
                }

                result.add(currentParam);
                previousParam = currentParam;
            }
        }

        return result;
    }

    static ContentLocation toProto(org.elasticsearch.xcontent.XContentLocation toProto) {
        if (toProto == null) {
            return null;
        }
        return new ContentLocation(toProto.lineNumber(), toProto.columnNumber());
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }

    protected static void validateParams(List<TypedParamValue> params) {
        for (TypedParamValue param : params) {
            if (param.hasExplicitType()) {
                throw new XContentParseException(
                    fromProto(param.tokenLocation()),
                    "[params] must be an array where each entry is a single field (no " + "objects supported)"
                );
            }
        }
    }

    static org.elasticsearch.xcontent.XContentLocation fromProto(ContentLocation fromProto) {
        if (fromProto == null) {
            return null;
        }
        return new org.elasticsearch.xcontent.XContentLocation(fromProto.lineNumber, fromProto.columnNumber);
    }

}
