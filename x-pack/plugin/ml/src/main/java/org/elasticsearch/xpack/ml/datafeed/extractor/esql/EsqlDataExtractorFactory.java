/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;

import java.util.Objects;

public class EsqlDataExtractorFactory implements DataExtractorFactory {

    private final Client client;
    private final DatafeedConfig datafeed;
    private final String timeField;

    private EsqlDataExtractorFactory(Client client, DatafeedConfig datafeed, String timeField) {
        this.client = Objects.requireNonNull(client);
        this.datafeed = Objects.requireNonNull(datafeed);
        this.timeField = timeField;
    }

    public static void create(
        Client client,
        DatafeedConfig datafeed,
        String timeField,
        ActionListener<DataExtractorFactory> factoryHandler
    ) {
        DataExtractorFactory factory = new EsqlDataExtractorFactory(client, datafeed, timeField);
        factoryHandler.onResponse(factory);
    }

    @Override
    public DataExtractor newExtractor(long start, long end) {
        return new EsqlDataExtractor(client, datafeed, timeField, start, end);
    }
}
