package org.elasticsearch.gradle.test;

import org.gradle.testkit.runner.GradleRunner;

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

    protected GradleRunner getGradleRunner(String sampleProject) {
        return GradleRunner.create()
            .withProjectDir(getProjectDir(sampleProject))
            .withPluginClasspath(
                //Collections.singleton(
                //    new File("/home/alpar/work/elastic/elasticsearch/buildSrc/build/pluginUnderTestMetaDataWithJar/")
                //)
            );
    }

}
