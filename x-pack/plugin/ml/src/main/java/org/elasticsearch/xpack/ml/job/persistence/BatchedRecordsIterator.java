/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Result;

import java.io.IOException;
import java.io.InputStream;

class BatchedRecordsIterator extends BatchedResultsIterator<AnomalyRecord> {

    BatchedRecordsIterator(OriginSettingClient client, String jobId) {
        super(client, jobId, AnomalyRecord.RESULT_TYPE_VALUE);
    }

    @Override
    protected Result<AnomalyRecord> map(SearchHit hit) {
        BytesReference source = hit.getSourceRef();
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, stream)){
            AnomalyRecord record = AnomalyRecord.LENIENT_PARSER.apply(parser, null);
            return new Result<>(hit.getIndex(), record);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse record", e);
        }
    }
}
