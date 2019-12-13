package org.elasticsearch.version.api;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;

//TODO: figure out security manager
// ./gradlew :qa:version-api:compatTests  -Dtests.timestamp=$(date +%S) --info -Dtests.security.manager=false
public class VersionApiClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {

    public VersionApiClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }



    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        //add how to find the tests here, if need to coordinate with gradle, use system properties
        String a = System.getProperty("foo", "notfound");
        System.out.println("************" + a);



       return ESClientYamlSuiteTestCase.createParameters(a);
    }




}
