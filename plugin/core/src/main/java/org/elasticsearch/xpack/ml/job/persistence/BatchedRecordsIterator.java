/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.Result;

import java.io.IOException;

class BatchedRecordsIterator extends BatchedResultsIterator<AnomalyRecord> {

    BatchedRecordsIterator(Client client, String jobId) {
        super(client, jobId, AnomalyRecord.RESULT_TYPE_VALUE);
    }

    @Override
    protected Result<AnomalyRecord> map(SearchHit hit) {
        BytesReference source = hit.getSourceRef();
        XContentParser parser;
        try {
            parser = XContentFactory.xContent(source).createParser(NamedXContentRegistry.EMPTY, source);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse record", e);
        }
        AnomalyRecord record = AnomalyRecord.PARSER.apply(parser, null);
        return new Result<>(hit.getIndex(), record);
    }
}