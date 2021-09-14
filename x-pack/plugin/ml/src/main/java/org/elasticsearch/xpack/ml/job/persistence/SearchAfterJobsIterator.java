/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.utils.persistence.SearchAfterDocumentsIterator;

import java.io.IOException;
import java.io.InputStream;

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
            return new Object[] {lastJobId};
        }
    }

    @Override
    protected void extractSearchAfterFields(SearchHit lastSearchHit) {
        lastJobId = Job.extractJobIdFromDocumentId(lastSearchHit.getId());
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
