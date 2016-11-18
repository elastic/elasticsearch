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

import org.elasticsearch.xpack.prelert.job.results.ModelDebugOutput;

class ElasticsearchBatchedModelDebugOutputIterator extends ElasticsearchBatchedDocumentsIterator<ModelDebugOutput>
{
    public ElasticsearchBatchedModelDebugOutputIterator(Client client, String jobId, ParseFieldMatcher parserFieldMatcher)
    {
        super(client, ElasticsearchPersister.getJobIndexName(jobId), parserFieldMatcher);
    }

    @Override
    protected String getType()
    {
        return ModelDebugOutput.TYPE.getPreferredName();
    }

    @Override
    protected ModelDebugOutput map(SearchHit hit)
    {
        BytesReference source = hit.getSourceRef();
        XContentParser parser;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parser model debug output", e);
        }
        ModelDebugOutput result = ModelDebugOutput.PARSER.apply(parser, () -> parseFieldMatcher);
        result.setId(hit.getId());
        return result;
    }
}
