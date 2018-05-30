package org.elasticsearch.gradle;

import org.junit.Test;

import java.io.File;

public class GradleIntegrationTests extends GradleUnitTestCase {

    protected File getProjectDir(String name) {
        File root = new File("src/test/resources/");
        if (root.exists() == false) {
            throw new RuntimeException("Could not find resources dir for integration tests. " +
                "Note that these tests can only be ran by Gradle and are not currently supported by the IDE");
        }
        return new File(root, name);
    }

    @Test
    public void  pass() {
    }
}
