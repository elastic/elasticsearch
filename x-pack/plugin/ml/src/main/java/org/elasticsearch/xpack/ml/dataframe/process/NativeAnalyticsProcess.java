/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.process.StateToProcessWriterHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class NativeAnalyticsProcess extends AbstractNativeAnalyticsProcess<AnalyticsResult> {

    private static final String NAME = "analytics";

    private final AnalyticsProcessConfig config;

    protected NativeAnalyticsProcess(String jobId, NativeController nativeController, ProcessPipes processPipes,
                                     int numberOfFields, List<Path> filesToDelete, Consumer<String> onProcessCrash,
                                     Duration processConnectTimeout, AnalyticsProcessConfig config,
                                     NamedXContentRegistry namedXContentRegistry) {
        super(NAME, AnalyticsResult.PARSER, jobId, nativeController, processPipes, numberOfFields, filesToDelete, onProcessCrash,
            processConnectTimeout, namedXContentRegistry);
        this.config = Objects.requireNonNull(config);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void persistState() {
        // Nothing to persist
    }

    @Override
    public void writeEndOfDataMessage() throws IOException {
        new AnalyticsControlMessageWriter(recordWriter(), numberOfFields()).writeEndOfData();
    }

    @Override
    public AnalyticsProcessConfig getConfig() {
        return config;
    }

    @Override
    public void restoreState(Client client, String stateDocIdPrefix) throws IOException {
        Objects.requireNonNull(stateDocIdPrefix);
        try (OutputStream restoreStream = processRestoreStream()) {
            int docNum = 0;
            while (true) {
                if (isProcessKilled()) {
                    return;
                }

                SearchResponse stateResponse = client.prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
                    .setSize(1)
                    .setQuery(QueryBuilders.idsQuery().addIds(stateDocIdPrefix + ++docNum)).get();
                if (stateResponse.getHits().getHits().length == 0) {
                    break;
                }
                StateToProcessWriterHelper.writeStateToStream(stateResponse.getHits().getAt(0).getSourceRef(), restoreStream);
            }
        }
    }
}
