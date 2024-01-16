/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.ApiCallSection;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
        return parameters(doSection -> {
            ApiCallSection copy = doSection.getApiCallSection().copyWithNewApi("esql.async_query");
            for (Map<String, Object> body : copy.getBodies()) {
                body.put("wait_for_completion_timeout", "30m");
            }
            doSection.setApiCallSection(copy);
            return doSection;
        });
    }

    public static Iterable<Object[]> parameters(Function<DoSection, ExecutableSection> modify) throws Exception {
        List<Object[]> result = new ArrayList<>();
        for (Object[] orig : ESClientYamlSuiteTestCase.createParameters()) {
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
