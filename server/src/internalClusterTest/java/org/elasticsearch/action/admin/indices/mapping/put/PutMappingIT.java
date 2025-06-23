/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.put;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import static org.hamcrest.Matchers.equalTo;

public class PutMappingIT extends ESSingleNodeTestCase {

    @TestLogging(
        reason = "testing DEBUG logging",
        value = "org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction:DEBUG"
    )
    public void testFailureLogging() {
        final var indexName = randomIdentifier();
        createIndex(indexName);
        final var fieldName = randomIdentifier();
        safeGet(client().execute(TransportPutMappingAction.TYPE, new PutMappingRequest(indexName).source(fieldName, "type=keyword")));
        MockLog.assertThatLogger(
            () -> assertThat(
                asInstanceOf(
                    IllegalArgumentException.class,
                    safeAwaitFailure(
                        AcknowledgedResponse.class,
                        l -> client().execute(
                            TransportPutMappingAction.TYPE,
                            new PutMappingRequest(indexName).source(fieldName, "type=long"),
                            l
                        )
                    )
                ).getMessage(),
                equalTo("mapper [" + fieldName + "] cannot be changed from type [keyword] to [long]")
            ),
            TransportPutMappingAction.class,
            new MockLog.SeenEventExpectation(
                "failure message",
                TransportPutMappingAction.class.getCanonicalName(),
                Level.DEBUG,
                "failed to put mappings on indices [[" + indexName
            )
        );
    }
}
