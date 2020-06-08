/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.utils.persistence.BatchedDocumentsIterator;

import java.io.IOException;
import java.io.InputStream;

public class BatchedJobsIterator extends BatchedDocumentsIterator<Job.Builder> {

    private final String jobIdExpression;

    public BatchedJobsIterator(OriginSettingClient client, String index, String jobIdExpression) {
        super(client, index);
        this.jobIdExpression = jobIdExpression;
    }

    @Override
    protected QueryBuilder getQuery() {
        String [] tokens = Strings.tokenizeToStringArray(jobIdExpression, ",");
        return JobConfigProvider.buildJobWildcardQuery(tokens, true);
    }

    @Override
    protected Job.Builder map(SearchHit hit) {
        try (InputStream stream = hit.getSourceRef().streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
            return Job.LENIENT_PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse job document [" + hit.getId() + "]", e);
        }
    }
}
