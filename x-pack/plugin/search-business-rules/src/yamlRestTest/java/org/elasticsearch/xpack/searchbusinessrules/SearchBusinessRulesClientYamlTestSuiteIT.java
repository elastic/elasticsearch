package org.elasticsearch.xpack.searchbusinessrules;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;

public class SearchBusinessRulesClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public SearchBusinessRulesClientYamlTestSuiteIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }
} 