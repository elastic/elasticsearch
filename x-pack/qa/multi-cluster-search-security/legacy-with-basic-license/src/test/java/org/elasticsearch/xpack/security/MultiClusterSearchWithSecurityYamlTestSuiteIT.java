/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
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

public class MultiClusterSearchWithSecurityYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    private static final String ESQL_VERSION = "2024.04.01";

    private static final String USER = "test_user";
    private static final String PASS = "x-pack-test-password";

    @Override
    protected boolean resetFeatureStates() {
        return false;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    public MultiClusterSearchWithSecurityYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return updateEsqlQueryDoSections(createParameters(), MultiClusterSearchWithSecurityYamlTestSuiteIT::setEsqlVersion);
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    // TODO: refactor, copied from ESQL's single-node AbstractEsqlClientYamlIt
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

    // TODO: refactor, copied from ESQL's single-node AbstractEsqlClientYamlIt
    private static ExecutableSection modifyExecutableSection(ExecutableSection e, Function<DoSection, ExecutableSection> modify) {
        if (false == (e instanceof DoSection)) {
            return e;
        }
        DoSection doSection = (DoSection) e;
        String api = doSection.getApiCallSection().getApi();
        return switch (api) {
            case "esql.query" -> modify.apply(doSection);
            default -> e;
        };
    }

    // TODO: refactor, copied from ESQL's single-node AbstractEsqlClientYamlIt
    static DoSection setEsqlVersion(DoSection doSection) {
        ApiCallSection copy = doSection.getApiCallSection().copyWithNewApi(doSection.getApiCallSection().getApi());
        for (Map<String, Object> body : copy.getBodies()) {
            body.putIfAbsent("version", ESQL_VERSION);
        }
        doSection.setApiCallSection(copy);
        return doSection;
    }
}
