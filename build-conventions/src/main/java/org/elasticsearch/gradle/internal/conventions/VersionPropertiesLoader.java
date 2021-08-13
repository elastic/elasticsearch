/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.conventions;


import org.gradle.api.provider.ProviderFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

// Define this here because we need it early.
public class VersionPropertiesLoader {
    static Properties loadBuildSrcVersion(File input, ProviderFactory providerFactory) throws IOException {
        Properties props = new Properties();
        InputStream is = new FileInputStream(input);
        try {
            props.load(is);
        } finally {
            is.close();
        }
        loadBuildSrcVersion(props, providerFactory);
        return props;
    }

    protected static void loadBuildSrcVersion(Properties loadedProps, ProviderFactory providers) {
        String elasticsearch = loadedProps.getProperty("elasticsearch");
        if (elasticsearch == null) {
            throw new IllegalStateException("Elasticsearch version is missing from properties.");
        }
        if (elasticsearch.matches("[0-9]+\\.[0-9]+\\.[0-9]+") == false) {
            throw new IllegalStateException(
                    "Expected elasticsearch version to be numbers only of the form  X.Y.Z but it was: " +
                            elasticsearch
            );
        }
        String qualifier = providers.systemProperty("build.version_qualifier")
                .orElse("")
                .forUseAtConfigurationTime()
                .get();
        if (qualifier.isEmpty() == false) {
            if (qualifier.matches("(alpha|beta|rc)\\d+") == false) {
                throw new IllegalStateException("Invalid qualifier: " + qualifier);
            }
            elasticsearch += "-" + qualifier;
        }
        final String buildSnapshotSystemProperty = providers.systemProperty("build.snapshot")
                .orElse("true")
                .forUseAtConfigurationTime()
                .get();
        switch (buildSnapshotSystemProperty) {
            case "true":
                elasticsearch += "-SNAPSHOT";
                break;
            case "false":
                // do nothing
                break;
            default:
                throw new IllegalArgumentException(
                        "build.snapshot was set to [" + buildSnapshotSystemProperty + "] but can only be unset or [true|false]");
        }
        loadedProps.put("elasticsearch", elasticsearch);
    }
}

