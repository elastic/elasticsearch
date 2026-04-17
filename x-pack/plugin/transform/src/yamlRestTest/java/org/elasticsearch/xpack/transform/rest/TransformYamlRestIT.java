/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.rest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.junit.ClassRule;

import java.util.ArrayList;
import java.util.List;

public class TransformYamlRestIT extends ESClientYamlSuiteTestCase {

    private static final String AUTH_VALUE = basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    public TransformYamlRestIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", AUTH_VALUE).build();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return wrapStartTransformWithWait(createParameters());
    }

    private static Iterable<Object[]> wrapStartTransformWithWait(Iterable<Object[]> parameters) {
        List<Object[]> result = new ArrayList<>();
        for (Object[] orig : parameters) {
            assert orig.length == 1;
            ClientYamlTestCandidate candidate = (ClientYamlTestCandidate) orig[0];
            var testSection = candidate.getTestSection();
            List<ExecutableSection> updated = testSection.getExecutableSections().stream().map(section -> {
                if (section instanceof DoSection doSection && "transform.start_transform".equals(doSection.getApiCallSection().getApi())) {
                    return new DoStartTransformAndWait(doSection, TransformYamlRestIT::adminClient);
                }
                return section;
            }).toList();
            ClientYamlTestSection modified = new ClientYamlTestSection(
                testSection.getLocation(),
                testSection.getName(),
                testSection.getPrerequisiteSection(),
                updated
            );
            result.add(new Object[] { new ClientYamlTestCandidate(candidate.getRestTestSuite(), modified) });
        }
        return result;
    }
}
