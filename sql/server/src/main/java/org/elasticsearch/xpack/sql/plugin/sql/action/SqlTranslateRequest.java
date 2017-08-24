/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.joda.time.DateTimeZone;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class SqlTranslateRequest extends AbstractSqlRequest {

    public static final ObjectParser<SqlTranslateRequest, Void> PARSER = objectParser(SqlTranslateRequest::new);

    public SqlTranslateRequest() {}

    public SqlTranslateRequest(String query, DateTimeZone timeZone, int fetchSize) {
        super(query, timeZone, fetchSize);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if ((false == Strings.hasText(query()))) {
            validationException = addValidationError("query is required", validationException);
        }
        return validationException;
    }

    @Override
    public String getDescription() {
        return "SQL Translate [" + query() + "]";
    }
}