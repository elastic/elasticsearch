/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;

import org.elasticsearch.xpack.prelert.job.results.Influencer;

class ElasticsearchBatchedInfluencersIterator extends ElasticsearchBatchedResultsIterator<Influencer>
{
    public ElasticsearchBatchedInfluencersIterator(Client client, String jobId,
            ParseFieldMatcher parserFieldMatcher)
    {
        super(client, jobId, Influencer.RESULT_TYPE_VALUE, parserFieldMatcher);
    }

    @Override
    protected Influencer map(SearchHit hit)
    {
        BytesReference source = hit.getSourceRef();
        XContentParser parser;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parser influencer", e);
        }

        Influencer influencer = Influencer.PARSER.apply(parser, () -> parseFieldMatcher);
        influencer.setId(hit.getId());
        return influencer;
    }
}
