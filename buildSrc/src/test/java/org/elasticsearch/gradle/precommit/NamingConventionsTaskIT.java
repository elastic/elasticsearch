package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.test.GradleIntegrationTestCase;
import org.gradle.testkit.runner.BuildResult;

public class NamingConventionsTaskIT extends GradleIntegrationTestCase {

    public void testNameCheckFailsAsItShould() {
        BuildResult result = getGradleRunner("namingConventionsSelfTest")
            .withArguments("namingConventions", "-s", "-PcheckForTestsInMain=false")
            .buildAndFail();

        assertTaskFailed(result, ":namingConventions");
        assertOutputContains(
            result.getOutput(),
            "Found inner classes that are tests, which are excluded from the test runner:",
            "* org.elasticsearch.test.NamingConventionsCheckInMainIT$InternalInvalidTests",
            "Classes ending with [Tests] must subclass [UnitTestCase]:",
            "* org.elasticsearch.test.NamingConventionsCheckInMainTests",
            "* org.elasticsearch.test.NamingConventionsCheckInMainIT",
            "Not all subclasses of UnitTestCase match the naming convention. Concrete classes must end with [Tests]:",
            "* org.elasticsearch.test.WrongName"
        );
    }

    public void testNameCheckFailsAsItShouldWithMain() {
        BuildResult result = getGradleRunner("namingConventionsSelfTest")
            .withArguments("namingConventions", "-s", "-PcheckForTestsInMain=true")
            .buildAndFail();

        assertTaskFailed(result, ":namingConventions");
        assertOutputContains(
            result.getOutput(),
            "Classes ending with [Tests] or [IT] or extending [UnitTestCase] must be in src/test/java:",
            "* org.elasticsearch.test.NamingConventionsCheckBadClasses$DummyInterfaceTests",
            "* org.elasticsearch.test.NamingConventionsCheckBadClasses$DummyAbstractTests",
            "* org.elasticsearch.test.NamingConventionsCheckBadClasses$InnerTests",
            "* org.elasticsearch.test.NamingConventionsCheckBadClasses$NotImplementingTests",
            "* org.elasticsearch.test.NamingConventionsCheckBadClasses$WrongNameTheSecond",
            "* org.elasticsearch.test.NamingConventionsCheckBadClasses$WrongName"
        );
    }

}
