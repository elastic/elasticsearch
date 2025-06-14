/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

abstract class AbstractEsqlClientYamlIT extends ESClientYamlSuiteTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @Override
    protected final String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected AbstractEsqlClientYamlIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Before
    @After
    private void assertRequestBreakerEmpty() throws Exception {
        /*
         * This hook is shared by all subclasses. If it is public it we'll
         * get complaints that it is inherited. It isn't. Whatever. Making
         * it private works - the hook still runs. It just looks strange.
         */
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }

    public static Iterable<Object[]> partialResultsDisablingParameters() throws Exception {
        return updateEsqlQueryDoSections(createParameters(), AbstractEsqlClientYamlIT::modifyEsqlQueryExecutableSection);
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

    public static Iterable<Object[]> updateEsqlQueryDoSections(Iterable<Object[]> parameters, Function<DoSection, ExecutableSection> modify)
        throws Exception {
        List<Object[]> result = new ArrayList<>();
        for (Object[] orig : parameters) {
            assert orig.length == 1;
            ClientYamlTestCandidate candidate = (ClientYamlTestCandidate) orig[0];
            try {
                ClientYamlTestSection modified = new ClientYamlTestSection(
                    candidate.getTestSection().getLocation(),
                    candidate.getTestSection().getName(),
                    candidate.getTestSection().getPrerequisiteSection(),
                    candidate.getTestSection().getExecutableSections().stream().map(e -> modifyExecutableSection(e, modify)).toList()
                );
                result.add(new Object[] { new ClientYamlTestCandidate(candidate.getRestTestSuite(), modified) });
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("error modifying " + candidate + ": " + e.getMessage(), e);
            }
        }
        return result;
    }

    private static ExecutableSection modifyExecutableSection(ExecutableSection e, Function<DoSection, ExecutableSection> modify) {
        if (false == (e instanceof DoSection)) {
            return e;
        }
        DoSection doSection = (DoSection) e;
        String api = doSection.getApiCallSection().getApi();
        return switch (api) {
            case "esql.query" -> modify.apply(doSection);
            case "esql.async_query", "esql.async_query_get" -> throw new IllegalArgumentException(
                "The esql yaml tests can't contain async_query or async_query_get because we modify them on the fly and *add* those."
            );
            default -> e;
        };
    }
}
