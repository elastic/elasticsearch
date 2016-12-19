/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.job.results.Influencer;

class ElasticsearchBatchedInfluencersIterator extends ElasticsearchBatchedResultsIterator<Influencer> {
    public ElasticsearchBatchedInfluencersIterator(Client client, String jobId,
                                                   ParseFieldMatcher parserFieldMatcher) {
        super(client, jobId, Influencer.RESULT_TYPE_VALUE, parserFieldMatcher);
    }

    @Override
    protected Influencer map(SearchHit hit) {
        BytesReference source = hit.getSourceRef();
        XContentParser parser;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parser influencer", e);
        }

        return Influencer.PARSER.apply(parser, () -> parseFieldMatcher);
    }
}
