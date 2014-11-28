package org.elasticsearch.common.logging.log4j;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.*;
import com.google.common.io.Files;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.test.ElasticsearchThreadFilter;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import sun.jvm.hotspot.utilities.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 *
 * This test class avoids the "java.lang.AssertionError: System properties invariant violated."
 * which is enforced by Junit Rules defined in AbstractRandomizedTest class.
 */
@RunWith(value = com.carrotsearch.randomizedtesting.RandomizedRunner.class)
public class CustomLogfileTest {

    private RandomizedTest randomizedTest = new RandomizedTest();

    @After
    public void clearProperty() {
        System.clearProperty("es.logging");
    }

    @Test
    public void testIncorrectLogFileNameIsNotLoaded() throws IOException {

        File customLoggingDir = randomizedTest.newTempDir();

        File customLogConfig = new File(customLoggingDir, "custom_log_config.yml");
        Files.write("foo: TRACE", customLogConfig, StandardCharsets.UTF_8);

        // the file directory
        System.setProperty("es.logging", customLogConfig.getAbsolutePath());

        Environment environment = new Environment(ImmutableSettings.builder().build());
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();

        assertThat(logSettings.get("foo"), isEmptyOrNullString());
    }

    @Test(expected = FailedToResolveConfigException.class)
    public void testIncorrectLogFileNameIsNotLoadedFromClasspath_1() throws IOException {

        File customLoggingDir = randomizedTest.newTempDir();

        File loggingFile = new File(customLoggingDir, "custom_log.yml");
        Files.write("foo: bar", loggingFile, StandardCharsets.UTF_8);

        String strClassPath = System.getProperty("java.class.path");
        System.setProperty("java.class.path", strClassPath + ":" + customLoggingDir);

        //the file name
        System.setProperty("es.logging", "custom_log.yml");

        Environment environment = new Environment(ImmutableSettings.builder().build());
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);
    }

    @Test
    public void testCorrectLogfileIsLoaded() throws IOException {

        File customLoggingDir = randomizedTest.newTempDir();

        File loggingFile = new File(customLoggingDir, "logging.yml");
        Files.write("foo: boo", loggingFile, StandardCharsets.UTF_8);

        System.setProperty("es.logging", customLoggingDir.getAbsolutePath());

        Environment environment = new Environment(ImmutableSettings.builder().build());
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);
        Settings logSettings = builder.build();

        assertThat(logSettings.get("foo"), is("boo"));
    }

    /**
     *
     * @throws Exception
     * @see LoggingConfigurationTests#testResolveYamlLoggingConfig() - similar but this test case
     *  uses the system property es.logging
     */
    @Test
    public void testResolveYamlLoggingConfig() throws Exception {
        File tmpDir = randomizedTest.newTempDir();
        File loggingConf1 = new File(tmpDir, LoggingConfigurationTests.loggingConfiguration("yml"));
        File loggingConf2 = new File(tmpDir, LoggingConfigurationTests.loggingConfiguration("yaml"));
        Files.write("yml: bar", loggingConf1, StandardCharsets.UTF_8);
        Files.write("yaml: bar", loggingConf2, StandardCharsets.UTF_8);

        System.setProperty("es.logging", tmpDir.getAbsolutePath());

        Environment environment = new Environment(ImmutableSettings.builder().build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("yml"), is("bar"));
        assertThat(logSettings.get("yaml"), is("bar"));
    }
}
