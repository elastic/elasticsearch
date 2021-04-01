/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.textstructure.action.AbstractFindStructureRequest;
import org.elasticsearch.xpack.core.textstructure.action.FindFieldStructureAction;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinderManager;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.xpack.textstructure.TextStructurePlugin.BASE_PATH;

public class RestFindFieldStructureAction extends BaseRestHandler {

    private static final TimeValue DEFAULT_TIMEOUT = new TimeValue(25, TimeUnit.SECONDS);

    @Override
    public List<Route> routes() {
        return List.of(
            Route.builder(POST, BASE_PATH + "{" + FindFieldStructureAction.Request.INDEX.getPreferredName() + "}/find_field_structure")
                .build()
        );
    }

    @Override
    public String getName() {
        return "text_structure_find_field_structure_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {

        FindFieldStructureAction.Request request = FindFieldStructureAction.Request.fromXContent(restRequest.contentOrSourceParamParser());
        request.setIndices(restRequest.paramAsStringArray(FindFieldStructureAction.Request.INDEX.getPreferredName(), new String[0]));
        request.setLinesToSample(
            restRequest.paramAsInt(
                AbstractFindStructureRequest.LINES_TO_SAMPLE.getPreferredName(),
                TextStructureFinderManager.DEFAULT_IDEAL_SAMPLE_LINE_COUNT
            )
        );
        request.setLineMergeSizeLimit(
            restRequest.paramAsInt(
                AbstractFindStructureRequest.LINE_MERGE_SIZE_LIMIT.getPreferredName(),
                TextStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT
            )
        );
        request.setTimeout(
            TimeValue.parseTimeValue(
                restRequest.param(AbstractFindStructureRequest.TIMEOUT.getPreferredName()),
                DEFAULT_TIMEOUT,
                AbstractFindStructureRequest.TIMEOUT.getPreferredName()
            )
        );
        request.setCharset(restRequest.param(AbstractFindStructureRequest.CHARSET.getPreferredName()));
        request.setFormat(restRequest.param(AbstractFindStructureRequest.FORMAT.getPreferredName()));
        request.setColumnNames(restRequest.paramAsStringArray(AbstractFindStructureRequest.COLUMN_NAMES.getPreferredName(), null));
        request.setHasHeaderRow(restRequest.paramAsBoolean(AbstractFindStructureRequest.HAS_HEADER_ROW.getPreferredName(), null));
        request.setDelimiter(restRequest.param(AbstractFindStructureRequest.DELIMITER.getPreferredName()));
        request.setQuote(restRequest.param(AbstractFindStructureRequest.QUOTE.getPreferredName()));
        request.setShouldTrimFields(restRequest.paramAsBoolean(AbstractFindStructureRequest.SHOULD_TRIM_FIELDS.getPreferredName(), null));
        request.setGrokPattern(restRequest.param(AbstractFindStructureRequest.GROK_PATTERN.getPreferredName()));
        request.setTimestampFormat(restRequest.param(AbstractFindStructureRequest.TIMESTAMP_FORMAT.getPreferredName()));
        request.setTimestampField(restRequest.param(AbstractFindStructureRequest.TIMESTAMP_FIELD.getPreferredName()));

        return channel -> client.execute(FindFieldStructureAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }

    @Override
    protected Set<String> responseParams() {
        return Collections.singleton(TextStructure.EXPLAIN);
    }
}
