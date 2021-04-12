/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.termenum.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

public class TermEnumAction extends ActionType<TermEnumResponse> {

    public static final TermEnumAction INSTANCE = new TermEnumAction();
    public static final String NAME = "indices:data/read/xpack/termsenum/list";
    
    
    static final ParseField INDEX_FILTER = new ParseField("index_filter");

    private TermEnumAction() {
        super(NAME, TermEnumResponse::new);
    }

    public static TermEnumRequest fromXContent(XContentParser parser, String... indices) throws IOException {
        TermEnumRequest request = new TermEnumRequest(indices);
        PARSER.parse(parser, request, null);
        return request;
    }

    private static final ObjectParser<TermEnumRequest, Void> PARSER = new ObjectParser<>("terms_enum_request");
    static {
        PARSER.declareString(TermEnumRequest::field, new ParseField("field"));
        PARSER.declareString(TermEnumRequest::string, new ParseField("string"));
        PARSER.declareInt(TermEnumRequest::size, new ParseField("size"));
        PARSER.declareBoolean(TermEnumRequest::caseInsensitive, new ParseField("case_insensitive"));
        PARSER.declareInt(TermEnumRequest::timeoutInMillis, new ParseField("timeout"));
        PARSER.declareObject(TermEnumRequest::indexFilter, (p, context) -> parseInnerQueryBuilder(p),INDEX_FILTER);        
    }
}
