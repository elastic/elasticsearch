/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class PrevalidateNodeRemovalRequestTests extends ESTestCase {

    public void testValidate() {
        ActionRequestValidationException ex1 = PrevalidateNodeRemovalRequest.builder().build().validate();
        assertNotNull(ex1);
        assertThat(ex1.validationErrors(), equalTo(List.of(PrevalidateNodeRemovalRequest.VALIDATION_ERROR_MSG_NO_QUERY_PARAM)));

        ActionRequestValidationException ex2 = PrevalidateNodeRemovalRequest.builder().setNames("name1").setIds("id1").build().validate();
        assertNotNull(ex2);
        assertThat(ex2.validationErrors(), equalTo(List.of(PrevalidateNodeRemovalRequest.VALIDATION_ERROR_MSG_ONLY_ONE_QUERY_PARAM)));

        ActionRequestValidationException ex3 = PrevalidateNodeRemovalRequest.builder()
            .setNames("name1")
            .setExternalIds("id1")
            .build()
            .validate();
        assertNotNull(ex3);
        assertThat(ex3.validationErrors(), equalTo(List.of(PrevalidateNodeRemovalRequest.VALIDATION_ERROR_MSG_ONLY_ONE_QUERY_PARAM)));

        assertNull(PrevalidateNodeRemovalRequest.builder().setNames("name1").build().validate());
        assertNull(PrevalidateNodeRemovalRequest.builder().setIds("id1").build().validate());
        assertNull(PrevalidateNodeRemovalRequest.builder().setExternalIds("external_id1").build().validate());
    }
}
