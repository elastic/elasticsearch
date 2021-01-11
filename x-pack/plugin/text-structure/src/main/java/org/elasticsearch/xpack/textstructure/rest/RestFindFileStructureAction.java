/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.textstructure.rest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.textstructure.action.FindFileStructureAction;
import org.elasticsearch.xpack.core.textstructure.structurefinder.FileStructure;
import org.elasticsearch.xpack.textstructure.structurefinder.FileStructureFinderManager;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.textstructure.TextStructurePlugin.BASE_PATH;

public class RestFindFileStructureAction extends BaseRestHandler {

    private static final TimeValue DEFAULT_TIMEOUT = new TimeValue(25, TimeUnit.SECONDS);

    @Override
    public List<Route> routes() {
        return Collections.emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return Collections.singletonList(new ReplacedRoute(POST, BASE_PATH + "find_structure", POST, "/_ml/find_file_structure"));
    }

    @Override
    public String getName() {
        return "text_structure_find_structure_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {

        FindFileStructureAction.Request request = new FindFileStructureAction.Request();
        request.setLinesToSample(
            restRequest.paramAsInt(
                FindFileStructureAction.Request.LINES_TO_SAMPLE.getPreferredName(),
                FileStructureFinderManager.DEFAULT_IDEAL_SAMPLE_LINE_COUNT
            )
        );
        request.setLineMergeSizeLimit(
            restRequest.paramAsInt(
                FindFileStructureAction.Request.LINE_MERGE_SIZE_LIMIT.getPreferredName(),
                FileStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT
            )
        );
        request.setTimeout(
            TimeValue.parseTimeValue(
                restRequest.param(FindFileStructureAction.Request.TIMEOUT.getPreferredName()),
                DEFAULT_TIMEOUT,
                FindFileStructureAction.Request.TIMEOUT.getPreferredName()
            )
        );
        request.setCharset(restRequest.param(FindFileStructureAction.Request.CHARSET.getPreferredName()));
        request.setFormat(restRequest.param(FindFileStructureAction.Request.FORMAT.getPreferredName()));
        request.setColumnNames(restRequest.paramAsStringArray(FindFileStructureAction.Request.COLUMN_NAMES.getPreferredName(), null));
        request.setHasHeaderRow(restRequest.paramAsBoolean(FindFileStructureAction.Request.HAS_HEADER_ROW.getPreferredName(), null));
        request.setDelimiter(restRequest.param(FindFileStructureAction.Request.DELIMITER.getPreferredName()));
        request.setQuote(restRequest.param(FindFileStructureAction.Request.QUOTE.getPreferredName()));
        request.setShouldTrimFields(
            restRequest.paramAsBoolean(FindFileStructureAction.Request.SHOULD_TRIM_FIELDS.getPreferredName(), null)
        );
        request.setGrokPattern(restRequest.param(FindFileStructureAction.Request.GROK_PATTERN.getPreferredName()));
        request.setTimestampFormat(restRequest.param(FindFileStructureAction.Request.TIMESTAMP_FORMAT.getPreferredName()));
        request.setTimestampField(restRequest.param(FindFileStructureAction.Request.TIMESTAMP_FIELD.getPreferredName()));
        if (restRequest.hasContent()) {
            request.setSample(restRequest.content());
        } else {
            throw new ElasticsearchParseException("request body is required");
        }

        return channel -> client.execute(FindFileStructureAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(FileStructure.EXPLAIN);
    }
}
