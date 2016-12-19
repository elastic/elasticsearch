/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.job.results.Bucket;

class ElasticsearchBatchedBucketsIterator extends ElasticsearchBatchedResultsIterator<Bucket> {

    public ElasticsearchBatchedBucketsIterator(Client client, String jobId, ParseFieldMatcher parseFieldMatcher) {
        super(client, jobId, Bucket.RESULT_TYPE_VALUE, parseFieldMatcher);
    }

    @Override
    protected Bucket map(SearchHit hit) {
        BytesReference source = hit.getSourceRef();
        XContentParser parser;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parse bucket", e);
        }
        return Bucket.PARSER.apply(parser, () -> parseFieldMatcher);
    }
}
