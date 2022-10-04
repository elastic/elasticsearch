/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.relevancesearch.relevance;

import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class QueryConfigurationTests extends ESTestCase {

    public void testParsingFieldsAndBoosts() throws RelevanceSettingsService.RelevanceSettingsInvalidException {
        List<String> fields = List.of("foo^2.5", "bar^5", "foobar");
        QueryConfiguration queryConfiguration = new QueryConfiguration();
        queryConfiguration.parseFieldsAndBoosts(fields);

        Map<String, Float> expectedFieldsAndBoosts = Map.of("foo", 2.5f, "bar", 5f, "foobar", AbstractQueryBuilder.DEFAULT_BOOST);
        Map<String, Float> fieldsAndBoosts = queryConfiguration.getFieldsAndBoosts();
        assertEquals(expectedFieldsAndBoosts, fieldsAndBoosts);
    }

    public void testParsingFieldsAndBoostsWithInvalidBoost() throws RelevanceSettingsService.RelevanceSettingsInvalidException {
        List<String> fields = List.of("foo^bar");
        QueryConfiguration queryConfiguration = new QueryConfiguration();
        RelevanceSettingsService.RelevanceSettingsInvalidException e = expectThrows(
            RelevanceSettingsService.RelevanceSettingsInvalidException.class,
            () -> queryConfiguration.parseFieldsAndBoosts(fields)
        );
    }

}
