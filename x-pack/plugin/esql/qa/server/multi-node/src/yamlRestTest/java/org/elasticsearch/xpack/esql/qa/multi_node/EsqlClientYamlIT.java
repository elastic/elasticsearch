/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.ApiCallSection;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class EsqlClientYamlIT extends ESClientYamlSuiteTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public EsqlClientYamlIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return updateEsqlQueryDoSections(createParameters(), EsqlClientYamlIT::setVersion);
    }

    @Before
    @After
    public void assertRequestBreakerEmpty() throws Exception {
        EsqlSpecTestCase.assertRequestBreakerEmpty();
    }

    // TODO: refactor, copied from single-node's AbstractEsqlClientYamlIt
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

    // TODO: refactor, copied from single-node's AbstractEsqlClientYamlIt
    private static ExecutableSection modifyExecutableSection(ExecutableSection e, Function<DoSection, ExecutableSection> modify) {
        if (false == (e instanceof DoSection)) {
            return e;
        }
        DoSection doSection = (DoSection) e;
        String api = doSection.getApiCallSection().getApi();
        return switch (api) {
            case "esql.query" -> modify.apply(doSection);
            // case "esql.async_query", "esql.async_query_get" -> throw new IllegalArgumentException(
            // "The esql yaml tests can't contain async_query or async_query_get because we modify them on the fly and *add* those."
            // );
            default -> e;
        };
    }

    // TODO: refactor, copied from single-node's AbstractEsqlClientYamlIt
    static DoSection setVersion(DoSection doSection) {
        ApiCallSection copy = doSection.getApiCallSection().copyWithNewApi(doSection.getApiCallSection().getApi());
        for (Map<String, Object> body : copy.getBodies()) {
            body.putIfAbsent("version", EsqlTestUtils.LOWEST_ESQL_VERSION);
        }
        doSection.setApiCallSection(copy);
        return doSection;
    }
}
