/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.rest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.textstructure.action.FindStructureAction;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinderManager;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.textstructure.TextStructurePlugin.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestFindStructureAction extends BaseRestHandler {

    private static final TimeValue DEFAULT_TIMEOUT = new TimeValue(25, TimeUnit.SECONDS);

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, BASE_PATH + "find_structure").replaces(POST, "/_ml/find_file_structure", RestApiVersion.V_8).build()
        );
    }

    @Override
    public String getName() {
        return "text_structure_find_structure_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        FindStructureAction.Request request = new FindStructureAction.Request();
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
        request.setTimeout(
            TimeValue.parseTimeValue(
                restRequest.param(FindStructureAction.Request.TIMEOUT.getPreferredName()),
                DEFAULT_TIMEOUT,
                FindStructureAction.Request.TIMEOUT.getPreferredName()
            )
        );
        request.setCharset(restRequest.param(FindStructureAction.Request.CHARSET.getPreferredName()));
        request.setFormat(restRequest.param(FindStructureAction.Request.FORMAT.getPreferredName()));
        request.setColumnNames(restRequest.paramAsStringArray(FindStructureAction.Request.COLUMN_NAMES.getPreferredName(), null));
        request.setHasHeaderRow(restRequest.paramAsBoolean(FindStructureAction.Request.HAS_HEADER_ROW.getPreferredName(), null));
        request.setDelimiter(restRequest.param(FindStructureAction.Request.DELIMITER.getPreferredName()));
        request.setQuote(restRequest.param(FindStructureAction.Request.QUOTE.getPreferredName()));
        request.setShouldTrimFields(restRequest.paramAsBoolean(FindStructureAction.Request.SHOULD_TRIM_FIELDS.getPreferredName(), null));
        request.setGrokPattern(restRequest.param(FindStructureAction.Request.GROK_PATTERN.getPreferredName()));
        request.setEcsCompatibility(restRequest.param(FindStructureAction.Request.ECS_COMPATIBILITY.getPreferredName()));
        request.setTimestampFormat(restRequest.param(FindStructureAction.Request.TIMESTAMP_FORMAT.getPreferredName()));
        request.setTimestampField(restRequest.param(FindStructureAction.Request.TIMESTAMP_FIELD.getPreferredName()));
        if (restRequest.hasContent()) {
            request.setSample(restRequest.content());
        } else {
            throw new ElasticsearchParseException("request body is required");
        }

        return channel -> client.execute(FindStructureAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(TextStructure.EXPLAIN);
    }
}
