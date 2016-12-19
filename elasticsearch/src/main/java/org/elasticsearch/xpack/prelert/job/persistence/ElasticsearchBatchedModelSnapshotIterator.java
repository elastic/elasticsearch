/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
import org.elasticsearch.xpack.prelert.job.ModelSnapshot;

class ElasticsearchBatchedModelSnapshotIterator extends ElasticsearchBatchedDocumentsIterator<ModelSnapshot> {
    public ElasticsearchBatchedModelSnapshotIterator(Client client, String jobId, ParseFieldMatcher parserFieldMatcher) {
        super(client, AnomalyDetectorsIndex.getJobIndexName(jobId), parserFieldMatcher);
    }

    @Override
    protected String getType() {
        return ModelSnapshot.TYPE.getPreferredName();
    }

    @Override
    protected ModelSnapshot map(SearchHit hit) {
        BytesReference source = hit.getSourceRef();
        XContentParser parser;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to parser model snapshot", e);
        }

        return ModelSnapshot.PARSER.apply(parser, () -> parseFieldMatcher);
    }
}
