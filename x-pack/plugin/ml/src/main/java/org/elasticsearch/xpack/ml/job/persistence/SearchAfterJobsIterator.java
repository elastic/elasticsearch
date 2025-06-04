/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.utils.persistence.SearchAfterDocumentsIterator;

import java.io.IOException;

public class SearchAfterJobsIterator extends SearchAfterDocumentsIterator<Job.Builder> {

    private String lastJobId;

    public SearchAfterJobsIterator(OriginSettingClient client) {
        super(client, MlConfigIndex.indexName());
    }

    @Override
    protected QueryBuilder getQuery() {
        return new TermQueryBuilder(Job.JOB_TYPE.getPreferredName(), Job.ANOMALY_DETECTOR_JOB_TYPE);
    }

    @Override
    protected FieldSortBuilder sortField() {
        return new FieldSortBuilder(Job.ID.getPreferredName());
    }

    @Override
    protected Object[] searchAfterFields() {
        if (lastJobId == null) {
            return null;
        } else {
            return new Object[] { lastJobId };
        }
    }

    @Override
    protected void extractSearchAfterFields(SearchHit lastSearchHit) {
        lastJobId = Job.extractJobIdFromDocumentId(lastSearchHit.getId());
    }

    @Override
    protected Job.Builder map(SearchHit hit) {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(
                LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                hit.getSourceRef(),
                XContentType.JSON
            )
        ) {
            return Job.LENIENT_PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse job document [" + hit.getId() + "]", e);
        }
    }
}
