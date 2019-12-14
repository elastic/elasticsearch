package org.elasticsearch.version.api;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestClient;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;

import java.util.List;
import java.util.Set;

//TODO: figure out security manager
// ./gradlew :qa:version-api:compatTests  -Dtests.timestamp=$(date +%S) --info -Dtests.security.manager=false
public class VersionApiClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    //TODO: get rid of the rest-api-spec as part of the test name
    //These are test names from the the last minor
    static final Set<String> EXPECT_TYPE_WARNINGS = Set.of(
        "rest-api-spec/test/ingest/80_foreach/Test foreach Processor"
    );

    public VersionApiClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }


    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {

        return ESClientYamlSuiteTestCase.createParameters(System.getProperty("foo"));
    }

    @Override
    protected ClientYamlTestClient initClientYamlTestClient(ClientYamlSuiteRestSpec restSpec, RestClient restClient, List<HttpHost> hosts, Version esVersion, Version masterVersion) {
        return super.initClientYamlTestClient(restSpec, restClient, hosts, esVersion, masterVersion);
    }

    @Override
    public void additionalInit() {
        DoSection doSection = null;
        List<ExecutableSection> executableSections = getTestCandidate().getTestSection().getExecutableSections();
        for (ExecutableSection executableSection : executableSections) {
            if (executableSection instanceof DoSection) {
                doSection = (DoSection) executableSection;
            }

            if (EXPECT_TYPE_WARNINGS.contains(getTestCandidate().getTestPath())
                && doSection.getApiCallSection().getParams().get("type") != null ) {
                doSection.addExpectedWarningHeader("foobarbear");

            }

        }
    }


}

