package org.elasticsearch.version.api;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestClient;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.restspec.ClientYamlSuiteRestSpec;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;

//TODO: figure out security manager
// ./gradlew :qa:version-api:integTestRunner  -Dtests.timestamp=$(date +%S) --info
// ./gradlew ':qa:version-api:integTestRunner' --tests "org.elasticsearch.version.api.VersionApiClientYamlTestSuiteIT.test {yaml=ingest/80_foreach/Test foreach Processor}"
public class VersionApiClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    //These are test names from the the last minor
    static final Set<String> EXPECT_TYPE_WARNINGS = Set.of(
        "ingest/80_foreach/Test foreach Processor"
    );

    public VersionApiClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        //TODO: handle modules, plugins, rest-api-spec, and test , then merge all of the outputs together
        Path root = Paths.get(System.getProperty("versionApiTestRoot"), "modules", "ingest-common" , "src", "test", "resources", "rest-api-spec", "test");
        return ESClientYamlSuiteTestCase.createParameters(ExecutableSection.XCONTENT_REGISTRY, root);
    }

    @Override
    protected ClientYamlTestClient initClientYamlTestClient(ClientYamlSuiteRestSpec restSpec, RestClient restClient, List<HttpHost> hosts, Version esVersion, Version masterVersion) {
        return super.initClientYamlTestClient(restSpec, restClient, hosts, esVersion, masterVersion);
    }

    @Override
    public void overrideDoSection() {
        addVersionHeader();
        handleTypeWarnings();
    }

    private void addVersionHeader(){
        //TODO: figure exactly what this header should be ...
        getAllDoSections().forEach(d -> d.getApiCallSection().addHeaders(Collections.singletonMap("compatible-with","v7")));
    }
    private void handleTypeWarnings(){
        if (EXPECT_TYPE_WARNINGS.contains(getTestCandidate().getTestPath())) {
            List<DoSection> doSections = getDoSectionsByParam("type");
            doSections.forEach(d -> d.addExpectedWarningHeader("foobarbear"));
        }
    }
}

