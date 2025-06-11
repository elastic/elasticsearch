/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;

/**
 * Run the ESQL yaml tests against the synchronous API.
 */
public class EsqlClientYamlIT extends AbstractEsqlClientYamlIT {

    public EsqlClientYamlIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return updateEsqlQueryDoSections(createParameters(), EsqlClientYamlIT::modifyEsqlQueryExecutableSection);
    }

    private static ExecutableSection modifyEsqlQueryExecutableSection(DoSection doSection) {
        var apiCallSection = doSection.getApiCallSection();
        if (apiCallSection.getApi().equals("esql.query")) {
            if (apiCallSection.getParams().containsKey("allow_partial_results") == false) {
                apiCallSection.addParam("allow_partial_results", "false"); // we want any error to fail the test
            }
        }
        return doSection;
    }

}
