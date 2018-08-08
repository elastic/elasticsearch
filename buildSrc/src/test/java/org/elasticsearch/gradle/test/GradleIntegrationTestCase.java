package org.elasticsearch.gradle.test;

import java.io.File;

public abstract class GradleIntegrationTestCase extends GradleUnitTestCase {

    protected File getProjectDir(String name) {
        File root = new File("src/testKit/");
        if (root.exists() == false) {
            throw new RuntimeException("Could not find resources dir for integration tests. " +
                "Note that these tests can only be ran by Gradle and are not currently supported by the IDE");
        }
        return new File(root, name);
    }

}
