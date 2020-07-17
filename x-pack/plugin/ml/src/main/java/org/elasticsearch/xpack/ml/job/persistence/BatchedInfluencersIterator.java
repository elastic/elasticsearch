/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.xpack.core.ml.job.results.Influencer;
import org.elasticsearch.xpack.core.ml.job.results.Result;

import java.io.IOException;
import java.io.InputStream;

class BatchedInfluencersIterator extends BatchedResultsIterator<Influencer> {
    BatchedInfluencersIterator(OriginSettingClient client, String jobId) {
        super(client, jobId, Influencer.RESULT_TYPE_VALUE);
    }

    @Override
    protected Result<Influencer> map(SearchHit hit) {
        BytesReference source = hit.getSourceRef();
        try (InputStream stream = source.streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(NamedXContentRegistry.EMPTY,
                     LoggingDeprecationHandler.INSTANCE, stream)) {
            Influencer influencer = Influencer.LENIENT_PARSER.apply(parser, null);
            return new Result<>(hit.getIndex(), influencer);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parser influencer", e);
        }
    }
}
