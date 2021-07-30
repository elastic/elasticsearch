/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class DeleteTransformRequestTests extends ESTestCase {

    public void testValidate() {
        assertFalse(new DeleteTransformRequest("valid-id").validate().isPresent());
        assertThat(new DeleteTransformRequest(null).validate().get().getMessage(),
                containsString("transform id must not be null"));
    }
}
