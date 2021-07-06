/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.fieldsenum.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class FieldsEnumAction extends ActionType<FieldsEnumResponse> {

    public static final FieldsEnumAction INSTANCE = new FieldsEnumAction();
    public static final String NAME = "indices:data/read/xpack/fieldsenum/list";


    static final ParseField INDEX_FILTER = new ParseField("index_filter");
    static final ParseField TIMEOUT = new ParseField("timeout");

    private FieldsEnumAction() {
        super(NAME, FieldsEnumResponse::new);
    }

    public static FieldsEnumRequest fromXContent(XContentParser parser, String... indices) throws IOException {
        FieldsEnumRequest request = new FieldsEnumRequest(indices);
        PARSER.parse(parser, request, null);
        return request;
    }

    private static final ObjectParser<FieldsEnumRequest, Void> PARSER = new ObjectParser<>("fields_enum_request");
    static {
        PARSER.declareString(FieldsEnumRequest::string, new ParseField("string"));
        PARSER.declareInt(FieldsEnumRequest::size, new ParseField("size"));
        PARSER.declareBoolean(FieldsEnumRequest::caseInsensitive, new ParseField("case_insensitive"));
        PARSER.declareField(FieldsEnumRequest::timeout,
            (p, c) -> TimeValue.parseTimeValue(p.text(), TIMEOUT.getPreferredName()),
            TIMEOUT, ObjectParser.ValueType.STRING);
        PARSER.declareObject(FieldsEnumRequest::indexFilter, (p, context) -> parseInnerQueryBuilder(p),INDEX_FILTER);
    }
}
