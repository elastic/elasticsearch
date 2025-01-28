/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.textstructure.rest;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.core.textstructure.action.AbstractFindStructureRequest;
import org.elasticsearch.xpack.core.textstructure.action.FindFieldStructureAction;
import org.elasticsearch.xpack.core.textstructure.action.FindMessageStructureAction;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureAction;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinderManager;

import java.util.concurrent.TimeUnit;

public class RestFindStructureArgumentsParser {

    private static final TimeValue DEFAULT_TIMEOUT = new TimeValue(25, TimeUnit.SECONDS);

    static void parse(RestRequest restRequest, AbstractFindStructureRequest request) {
        if (request instanceof FindStructureAction.Request) {
            request.setLinesToSample(
                restRequest.paramAsInt(
                    FindStructureAction.Request.LINES_TO_SAMPLE.getPreferredName(),
                    TextStructureFinderManager.DEFAULT_IDEAL_SAMPLE_LINE_COUNT
                )
            );
            request.setLineMergeSizeLimit(
                restRequest.paramAsInt(
                    FindStructureAction.Request.LINE_MERGE_SIZE_LIMIT.getPreferredName(),
                    TextStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT
                )
            );
            request.setCharset(restRequest.param(FindStructureAction.Request.CHARSET.getPreferredName()));
            request.setHasHeaderRow(restRequest.paramAsBoolean(FindStructureAction.Request.HAS_HEADER_ROW.getPreferredName(), null));
        } else if (request instanceof FindFieldStructureAction.Request) {
            request.setLinesToSample(
                restRequest.paramAsInt(
                    FindStructureAction.Request.DOCUMENTS_TO_SAMPLE.getPreferredName(),
                    TextStructureFinderManager.DEFAULT_IDEAL_SAMPLE_LINE_COUNT
                )
            );
        }

        request.setTimeout(
            TimeValue.parseTimeValue(
                restRequest.param(FindStructureAction.Request.TIMEOUT.getPreferredName()),
                DEFAULT_TIMEOUT,
                FindStructureAction.Request.TIMEOUT.getPreferredName()
            )
        );
        request.setFormat(restRequest.param(FindStructureAction.Request.FORMAT.getPreferredName()));
        request.setColumnNames(restRequest.paramAsStringArray(FindStructureAction.Request.COLUMN_NAMES.getPreferredName(), null));
        request.setDelimiter(restRequest.param(FindStructureAction.Request.DELIMITER.getPreferredName()));
        request.setQuote(restRequest.param(FindStructureAction.Request.QUOTE.getPreferredName()));
        request.setShouldTrimFields(restRequest.paramAsBoolean(FindStructureAction.Request.SHOULD_TRIM_FIELDS.getPreferredName(), null));
        request.setGrokPattern(restRequest.param(FindStructureAction.Request.GROK_PATTERN.getPreferredName()));
        request.setEcsCompatibility(restRequest.param(FindStructureAction.Request.ECS_COMPATIBILITY.getPreferredName()));
        request.setTimestampFormat(restRequest.param(FindStructureAction.Request.TIMESTAMP_FORMAT.getPreferredName()));
        request.setTimestampField(restRequest.param(FindStructureAction.Request.TIMESTAMP_FIELD.getPreferredName()));

        if (request instanceof FindMessageStructureAction.Request || request instanceof FindFieldStructureAction.Request) {
            if (TextStructure.Format.DELIMITED.equals(request.getFormat())) {
                request.setHasHeaderRow(false);
            }
        }
    }
}
