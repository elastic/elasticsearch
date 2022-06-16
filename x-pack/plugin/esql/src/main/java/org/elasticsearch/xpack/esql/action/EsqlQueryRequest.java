/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.ZoneId;
import java.util.function.Supplier;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class EsqlQueryRequest extends ActionRequest implements CompositeIndicesRequest {

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField COLUMNAR_FIELD = new ParseField("columnar"); // TODO -> "mode"?
    private static final ParseField TIME_ZONE_FIELD = new ParseField("time_zone");

    private static final ObjectParser<EsqlQueryRequest, Void> PARSER = objectParser(EsqlQueryRequest::new);

    private String query;
    private boolean columnar;
    private ZoneId zoneId;

    public EsqlQueryRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.hasText(query) == false) {
            validationException = addValidationError("[query] is required", null);
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

    public static EsqlQueryRequest fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
    private static ObjectParser<EsqlQueryRequest, Void> objectParser(Supplier<EsqlQueryRequest> supplier) {
        ObjectParser<EsqlQueryRequest, Void> parser = new ObjectParser<>("esql/query", false, supplier);
        parser.declareString(EsqlQueryRequest::query, QUERY_FIELD);
        parser.declareBoolean(EsqlQueryRequest::columnar, COLUMNAR_FIELD);
        parser.declareString((request, zoneId) -> request.zoneId(ZoneId.of(zoneId)), TIME_ZONE_FIELD);
        return parser;
    }
}
