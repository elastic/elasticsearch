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
import org.elasticsearch.xpack.ml.job.results.Influencer;

import java.io.IOException;

class ElasticsearchBatchedInfluencersIterator extends ElasticsearchBatchedResultsIterator<Influencer> {
    public ElasticsearchBatchedInfluencersIterator(Client client, String jobId) {
        super(client, jobId, Influencer.RESULT_TYPE_VALUE);
    }

    @Override
    protected Influencer map(SearchHit hit) {
        BytesReference source = hit.getSourceRef();
        XContentParser parser;
        try {
            parser = XContentFactory.xContent(source).createParser(NamedXContentRegistry.EMPTY, source);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parser influencer", e);
        }

        return Influencer.PARSER.apply(parser, null);
    }
}
