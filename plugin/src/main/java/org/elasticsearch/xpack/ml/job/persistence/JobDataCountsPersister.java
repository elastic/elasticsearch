/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Update a job's dataCounts
 * i.e. the number of processed records, fields etc.
 */
public class JobDataCountsPersister extends AbstractComponent {

    private final Client client;

    public JobDataCountsPersister(Settings settings, Client client) {
        super(settings);
        this.client = client;
    }

    private XContentBuilder serialiseCounts(DataCounts counts) throws IOException {
        XContentBuilder builder = jsonBuilder();
        return counts.toXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    /**
     * Update the job's data counts stats and figures.
     *
     * @param jobId Job to update
     * @param counts The counts
     * @param listener Action response listener
     */
    public void persistDataCounts(String jobId, DataCounts counts, ActionListener<Boolean> listener) {
        try (XContentBuilder content = serialiseCounts(counts)) {
            client.prepareIndex(AnomalyDetectorsIndex.resultsWriteAlias(jobId), ElasticsearchMappings.DOC_TYPE,
                    DataCounts.documentId(jobId))
            .setSource(content).execute(new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    listener.onResponse(true);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } catch (IOException ioe) {
            logger.warn((Supplier<?>)() -> new ParameterizedMessage("[{}] Error serialising DataCounts stats", jobId), ioe);
        }
    }
}
