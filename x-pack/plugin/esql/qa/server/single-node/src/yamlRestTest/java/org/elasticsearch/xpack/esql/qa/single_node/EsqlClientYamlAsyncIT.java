/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.section.ApiCallSection;

import java.util.Map;

/**
 * Run the ESQL yaml tests against the async esql endpoint with a 30 minute {@code wait_until_completion_timeout}.
 * That's far longer than any should take and far longer than any sensible person will wait, but it's simple
 * and it makes sure all the yaml tests work when within the timeout.
 */
public class EsqlClientYamlAsyncIT extends AbstractEsqlClientYamlIT {
    public EsqlClientYamlAsyncIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return updateEsqlQueryDoSections(partialResultsCheckingParatmers(), doSection -> {
            ApiCallSection copy = doSection.getApiCallSection().copyWithNewApi("esql.async_query");
            for (Map<String, Object> body : copy.getBodies()) {
                body.put("wait_for_completion_timeout", "30m");
            }
            doSection.setApiCallSection(copy);
            return doSection;
        });
    }
}
