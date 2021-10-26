/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.test.eql.EqlRestValidationTestCase;

import java.io.IOException;

public class EqlRestValidationIT extends EqlRestValidationTestCase {

    @Override
    protected String getInexistentIndexErrorMessage() {
        return "\"root_cause\":[{\"type\":\"verification_exception\",\"reason\":\"Found 1 problem\\nline -1:-1: Unknown index ";
    }

    protected void assertErrorMessageWhenAllowNoIndicesIsFalse(String reqParameter) throws IOException {
        assertErrorMessage("inexistent1*", reqParameter, "\"root_cause\":[{\"type\":\"index_not_found_exception\","
            + "\"reason\":\"no such index [inexistent1*]\"");
        assertErrorMessage("inexistent1*,inexistent2*", reqParameter, "\"root_cause\":[{\"type\":\"index_not_found_exception\","
            + "\"reason\":\"no such index [inexistent1*]\"");
        assertErrorMessage("test_eql,inexistent*", reqParameter, "\"root_cause\":[{\"type\":\"index_not_found_exception\","
            + "\"reason\":\"no such index [inexistent*]\"");
        assertErrorMessage("inexistent", reqParameter, "\"root_cause\":[{\"type\":\"index_not_found_exception\","
            + "\"reason\":\"no such index [inexistent]\"");
        //TODO: revisit after https://github.com/elastic/elasticsearch/issues/64197 is closed
        assertErrorMessage("inexistent1,inexistent2", reqParameter, "\"root_cause\":[{\"type\":\"index_not_found_exception\","
            + "\"reason\":\"no such index [null]\"");
    }
}
