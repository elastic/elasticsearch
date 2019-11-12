package org.elasticsearch.client.tasks;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ElasticSearchExceptionTests extends AbstractResponseTestCase<org.elasticsearch.ElasticsearchException, org.elasticsearch.client.tasks.ElasticsearchException> {

    @Override
    protected org.elasticsearch.ElasticsearchException createServerTestInstance(XContentType xContentType) {
        IllegalStateException ies = new IllegalStateException("illegal_state");
        IllegalArgumentException iae = new IllegalArgumentException("argument", ies);
        org.elasticsearch.ElasticsearchException exception = new org.elasticsearch.ElasticsearchException("elastic_exception", iae);
        return exception;
    }

    @Override
    protected ElasticsearchException doParseToClientInstance(XContentParser parser) throws IOException {
        return ElasticsearchException.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.ElasticsearchException serverTestInstance, ElasticsearchException clientInstance) {

        String message = serverTestInstance.getDetailedMessage();
        String msg = clientInstance.getMsg();
    }

}
