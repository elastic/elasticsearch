/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.elasticsearch.common.Strings;
import org.hamcrest.Matchers;

import java.util.List;

public class KqlNestedFieldQueryTests extends AbstractKqlParserTestCase {

    public void testInvalidNestedFieldName() {
        for (String invalidFieldName : List.of(TEXT_FIELD_NAME, "not_a_field", "mapped_nest*")) {
            KqlParsingException e = assertThrows(
                KqlParsingException.class,
                () -> parseKqlQuery(Strings.format("%s : { %s: foo AND %s < 10 } ", invalidFieldName, TEXT_FIELD_NAME, INT_FIELD_NAME))
            );
            assertThat(e.getMessage(), Matchers.containsString(invalidFieldName));
            assertThat(e.getMessage(), Matchers.containsString("is not a valid nested field name"));
        }
    }
}
