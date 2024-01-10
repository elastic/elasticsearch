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
import org.elasticsearch.test.rest.yaml.section.DoSection;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Run the ESQL yaml tests async and then fetch the results with a long wait time.
 */
public class EsqlClientYamlAsyncSubmitAndFetchIT extends AbstractEsqlClientYamlIT {
    public EsqlClientYamlAsyncSubmitAndFetchIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return EsqlClientYamlAsyncIT.parameters(doSection -> {
            ApiCallSection copy = doSection.getApiCallSection().copyWithNewApi("esql.async_query");
            for (Map<String, Object> body : copy.getBodies()) {
                body.put("wait_for_completion_timeout", "0ms");
            }
            doSection.setApiCallSection(copy);

            DoSection fetch = new DoSection(doSection.getLocation());
            fetch.setApiCallSection(new ApiCallSection("esql.async_query_get"));
            fetch.getApiCallSection().addParam("wait_for_completion_timeout", "30m");
            fetch.getApiCallSection().addParam("id", "$body.id");

            /*
             * The request to start the query doesn't make warnings or errors so shift
             * those to the fetch.
             */
            fetch.setExpectedWarningHeaders(doSection.getExpectedWarningHeaders());
            fetch.setExpectedWarningHeadersRegex(doSection.getExpectedWarningHeadersRegex());
            fetch.setAllowedWarningHeaders(doSection.getAllowedWarningHeaders());
            fetch.setAllowedWarningHeadersRegex(doSection.getAllowedWarningHeadersRegex());
            fetch.setCatch(doSection.getCatch());
            doSection.setExpectedWarningHeaders(List.of());
            doSection.setExpectedWarningHeadersRegex(List.of());
            doSection.setAllowedWarningHeaders(List.of());
            doSection.setAllowedWarningHeadersRegex(List.of());
            doSection.setCatch(null);
            return Stream.of(doSection, fetch);
        });
    }
}
