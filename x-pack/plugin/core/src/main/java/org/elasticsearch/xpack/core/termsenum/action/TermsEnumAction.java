/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termsenum.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class TermsEnumAction extends ActionType<TermsEnumResponse> {

    public static final TermsEnumAction INSTANCE = new TermsEnumAction();
    public static final String NAME = "indices:data/read/xpack/termsenum/list";


    static final ParseField INDEX_FILTER = new ParseField("index_filter");
    static final ParseField TIMEOUT = new ParseField("timeout");

    private TermsEnumAction() {
        super(NAME, TermsEnumResponse::new);
    }

    public static TermsEnumRequest fromXContent(XContentParser parser, String... indices) throws IOException {
        TermsEnumRequest request = new TermsEnumRequest(indices);
        PARSER.parse(parser, request, null);
        return request;
    }

    private static final ObjectParser<TermsEnumRequest, Void> PARSER = new ObjectParser<>("terms_enum_request");
    static {
        PARSER.declareString(TermsEnumRequest::field, new ParseField("field"));
        PARSER.declareString(TermsEnumRequest::string, new ParseField("string"));
        PARSER.declareString(TermsEnumRequest::searchAfter, new ParseField("search_after"));
        PARSER.declareInt(TermsEnumRequest::size, new ParseField("size"));
        PARSER.declareBoolean(TermsEnumRequest::caseInsensitive, new ParseField("case_insensitive"));
        PARSER.declareField(TermsEnumRequest::timeout,
            (p, c) -> TimeValue.parseTimeValue(p.text(), TIMEOUT.getPreferredName()),
            TIMEOUT, ObjectParser.ValueType.STRING);
        PARSER.declareObject(TermsEnumRequest::indexFilter, (p, context) -> parseInnerQueryBuilder(p), INDEX_FILTER);
    }
}
