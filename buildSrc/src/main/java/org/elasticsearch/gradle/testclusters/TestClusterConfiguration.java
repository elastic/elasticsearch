/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.testclusters;

import org.elasticsearch.gradle.FileSupplier;
import org.elasticsearch.gradle.PropertyNormalization;
import org.gradle.api.file.RegularFile;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Provider;
import org.slf4j.Logger;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public interface TestClusterConfiguration {

    void setVersion(String version);

    void setVersions(List<String> version);

    void setTestDistribution(TestDistribution distribution);

    void plugin(Provider<RegularFile> plugin);

    void plugin(String pluginProjectPath);

    void module(Provider<RegularFile> module);

    void module(String moduleProjectPath);

    void keystore(String key, String value);

    void keystore(String key, Supplier<CharSequence> valueSupplier);

    void keystore(String key, File value);

    void keystore(String key, File value, PropertyNormalization normalization);

    void keystore(String key, FileSupplier valueSupplier);

    void keystorePassword(String password);

    void cliSetup(String binTool, CharSequence... args);

    void setting(String key, String value);

    void setting(String key, String value, PropertyNormalization normalization);

    void setting(String key, Supplier<CharSequence> valueSupplier);

    void setting(String key, Supplier<CharSequence> valueSupplier, PropertyNormalization normalization);

    void systemProperty(String key, String value);

    void systemProperty(String key, Supplier<CharSequence> valueSupplier);

    void systemProperty(String key, Supplier<CharSequence> valueSupplier, PropertyNormalization normalization);

    void environment(String key, String value);

    void environment(String key, Supplier<CharSequence> valueSupplier);

    void environment(String key, Supplier<CharSequence> valueSupplier, PropertyNormalization normalization);

    void jvmArgs(String... values);

    boolean isPreserveDataDir();

    void setPreserveDataDir(boolean preserveDataDir);

    void freeze();

    void start();

    void restart();

    void extraConfigFile(String destination, File from);

    void extraConfigFile(String destination, File from, PropertyNormalization normalization);

    void extraJarFile(File from);

    void user(Map<String, String> userSpec);

    String getHttpSocketURI();

    String getTransportPortURI();

    List<String> getAllHttpSocketURI();

    List<String> getAllTransportPortURI();

    void stop(boolean tailLogs);

    void setNameCustomization(Function<String, String> nameSupplier);

    default void waitForConditions(
        LinkedHashMap<String, Predicate<TestClusterConfiguration>> waitConditions,
        long startedAtMillis,
        long nodeUpTimeout,
        TimeUnit nodeUpTimeoutUnit,
        TestClusterConfiguration context
    ) {
        Logger logger = Logging.getLogger(TestClusterConfiguration.class);
        waitConditions.forEach((description, predicate) -> {
            long thisConditionStartedAt = System.currentTimeMillis();
            boolean conditionMet = false;
            Throwable lastException = null;
            while (System.currentTimeMillis() - startedAtMillis < TimeUnit.MILLISECONDS.convert(nodeUpTimeout, nodeUpTimeoutUnit)) {
                if (context.isProcessAlive() == false) {
                    throw new TestClustersException("process was found dead while waiting for " + description + ", " + this);
                }

                try {
                    if (predicate.test(context)) {
                        conditionMet = true;
                        break;
                    }
                } catch (TestClustersException e) {
                    throw e;
                } catch (Exception e) {
                    lastException = e;
                }
            }
            if (conditionMet == false) {
                String message = String.format(
                    Locale.ROOT,
                    "`%s` failed to wait for %s after %d %s",
                    context,
                    description,
                    nodeUpTimeout,
                    nodeUpTimeoutUnit
                );
                if (lastException == null) {
                    throw new TestClustersException(message);
                } else {
                    String extraCause = "";
                    Throwable cause = lastException;
                    int ident = 2;
                    while (cause != null) {
                        if (cause.getMessage() != null && cause.getMessage().isEmpty() == false) {
                            extraCause += "\n" + " ".repeat(ident) + cause.getMessage();
                            ident += 2;
                        }
                        cause = cause.getCause();
                    }
                    throw new TestClustersException(message + extraCause, lastException);
                }
            }
            logger.info("{}: {} took {} seconds", this, description, (System.currentTimeMillis() - thisConditionStartedAt) / 1000.0);
        });
    }

    default String safeName(String name) {
        return name.replaceAll("^[^a-zA-Z0-9]+", "").replaceAll("[^a-zA-Z0-9\\.]+", "-");
    }

    boolean isProcessAlive();
}
