/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.results.ReservedFieldNames;

import java.io.IOException;
import java.util.Objects;

class ElasticsearchJobDetailsMapper {
    private static final Logger LOGGER = Loggers.getLogger(ElasticsearchJobDetailsMapper.class);

    private final Client client;
    private final ParseFieldMatcher parseFieldMatcher;

    public ElasticsearchJobDetailsMapper(Client client, ParseFieldMatcher parseFieldMatcher) {
        this.client = Objects.requireNonNull(client);
        this.parseFieldMatcher = Objects.requireNonNull(parseFieldMatcher);
    }

    /**
     * Maps an Elasticsearch source map to a {@link Job} object
     *
     * @param source The source of an Elasticsearch search response
     * @return the {@code Job} object
     */
    public Job map(BytesReference source) {
        try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
            Job.Builder builder = Job.PARSER.apply(parser, () -> parseFieldMatcher);
            addModelSizeStats(builder, builder.getId());
            addBucketProcessingTime(builder, builder.getId());
            return builder.build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parser job", e);
        }
    }

    private void addModelSizeStats(Job.Builder job, String jobId) {
        String indexName = ElasticsearchPersister.getJobIndexName(jobId);
        // Pull out the modelSizeStats document, and add this to the Job
        LOGGER.trace("ES API CALL: get ID " + ModelSizeStats.TYPE +
                " type " + ModelSizeStats.TYPE + " from index " + indexName);
        GetResponse modelSizeStatsResponse = client.prepareGet(
                indexName, ModelSizeStats.TYPE.getPreferredName(), ModelSizeStats.TYPE.getPreferredName()).get();

        if (!modelSizeStatsResponse.isExists()) {
            String msg = "No memory usage details for job with id " + jobId;
            LOGGER.warn(msg);
        } else {
            // Remove the Kibana/Logstash '@timestamp' entry as stored in Elasticsearch,
            // and replace using the API 'timestamp' key.
            Object timestamp = modelSizeStatsResponse.getSource().remove(ElasticsearchMappings.ES_TIMESTAMP);
            modelSizeStatsResponse.getSource().put(ModelSizeStats.TIMESTAMP_FIELD.getPreferredName(), timestamp);
            BytesReference source = modelSizeStatsResponse.getSourceAsBytesRef();
            XContentParser parser;
            try {
                parser = XContentFactory.xContent(source).createParser(source);
            } catch (IOException e) {
                throw new ElasticsearchParseException("failed to parser model size stats", e);
            }
            ModelSizeStats.Builder modelSizeStats = ModelSizeStats.PARSER.apply(parser, () -> parseFieldMatcher);
            job.setModelSizeStats(modelSizeStats);
        }
    }

    private void addBucketProcessingTime(Job.Builder job, String jobId) {
        String indexName = ElasticsearchPersister.getJobIndexName(jobId);
        // Pull out the modelSizeStats document, and add this to the Job
        LOGGER.trace("ES API CALL: get ID " + ReservedFieldNames.BUCKET_PROCESSING_TIME_TYPE +
                " type " + ReservedFieldNames.AVERAGE_PROCESSING_TIME_MS + " from index " + indexName);
        GetResponse procTimeResponse = client.prepareGet(
                indexName, ReservedFieldNames.BUCKET_PROCESSING_TIME_TYPE,
                ReservedFieldNames.AVERAGE_PROCESSING_TIME_MS).get();

        if (!procTimeResponse.isExists()) {
            String msg = "No average bucket processing time details for job with id " + jobId;
            LOGGER.warn(msg);
        } else {
            Object averageTime = procTimeResponse.getSource()
                    .get(ReservedFieldNames.AVERAGE_PROCESSING_TIME_MS);
            if (averageTime instanceof Double) {
                job.setAverageBucketProcessingTimeMs((Double) averageTime);
            }
        }
    }
}
