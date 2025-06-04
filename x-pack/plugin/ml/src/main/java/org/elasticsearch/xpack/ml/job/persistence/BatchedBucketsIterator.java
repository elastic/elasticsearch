/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.Result;

import java.io.IOException;

class BatchedBucketsIterator extends BatchedResultsIterator<Bucket> {

    BatchedBucketsIterator(OriginSettingClient client, String jobId) {
        super(client, jobId, Bucket.RESULT_TYPE_VALUE);
    }

    @Override
    protected Result<Bucket> map(SearchHit hit) {
        try {
            try (
                XContentParser parser = XContentHelper.createParserNotCompressed(
                    LoggingDeprecationHandler.XCONTENT_PARSER_CONFIG,
                    hit.getSourceRef(),
                    XContentType.JSON
                )
            ) {
                Bucket bucket = Bucket.LENIENT_PARSER.apply(parser, null);
                return new Result<>(hit.getIndex(), bucket);
            }
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse bucket", e);
        }
    }
}
