/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.tasks;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

public class ElasticsearchExceptionTests extends AbstractResponseTestCase<org.elasticsearch.ElasticsearchException,
    org.elasticsearch.client.tasks.ElasticsearchException> {

    @Override
    protected org.elasticsearch.ElasticsearchException createServerTestInstance(XContentType xContentType) {
        IllegalStateException ies = new IllegalStateException("illegal_state");
        IllegalArgumentException iae = new IllegalArgumentException("argument", ies);
        org.elasticsearch.ElasticsearchException exception = new org.elasticsearch.ElasticsearchException("elastic_exception", iae);
        exception.addHeader("key","value");
        exception.addMetadata("es.meta","data");
        exception.addSuppressed(new NumberFormatException("3/0"));
        return exception;
    }

    @Override
    protected ElasticsearchException doParseToClientInstance(XContentParser parser) throws IOException {
        parser.nextToken();
        return ElasticsearchException.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.ElasticsearchException serverTestInstance, ElasticsearchException clientInstance) {

        IllegalArgumentException sCauseLevel1 = (IllegalArgumentException) serverTestInstance.getCause();
        ElasticsearchException cCauseLevel1 = clientInstance.getCause();

        assertTrue(sCauseLevel1 !=null);
        assertTrue(cCauseLevel1 !=null);

        IllegalStateException causeLevel2 = (IllegalStateException) serverTestInstance.getCause().getCause();
        ElasticsearchException cCauseLevel2 = clientInstance.getCause().getCause();
        assertTrue(causeLevel2 !=null);
        assertTrue(cCauseLevel2 !=null);


        ElasticsearchException cause = new ElasticsearchException(
            "Elasticsearch exception [type=illegal_state_exception, reason=illegal_state]"
        );
        ElasticsearchException caused1 = new ElasticsearchException(
            "Elasticsearch exception [type=illegal_argument_exception, reason=argument]",cause
        );
        ElasticsearchException caused2 = new ElasticsearchException(
            "Elasticsearch exception [type=exception, reason=elastic_exception]",caused1
        );

        caused2.addHeader("key", List.of("value"));
        ElasticsearchException supp = new ElasticsearchException(
            "Elasticsearch exception [type=number_format_exception, reason=3/0]"
        );
        caused2.addSuppressed(List.of(supp));

        assertEquals(caused2,clientInstance);

    }

}
