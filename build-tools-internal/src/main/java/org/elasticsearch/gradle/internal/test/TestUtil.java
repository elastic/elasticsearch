/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.Architecture;
import org.elasticsearch.gradle.ElasticsearchDistribution;

import java.io.File;
import java.util.Locale;

public class TestUtil {

    public static String getTestLibraryPath(String nativeLibsDir) {
        String arch = Architecture.current().toString().toLowerCase(Locale.ROOT);
        String platform = String.format(Locale.ROOT, "%s-%s", ElasticsearchDistribution.CURRENT_PLATFORM, arch);
        String existingLibraryPath = System.getProperty("java.library.path");

        String format = "%s/%s%c%s";
        if (ElasticsearchDistribution.CURRENT_PLATFORM.equals(ElasticsearchDistribution.Platform.WINDOWS)) {
            // windows doesn't like spaces in paths, so we must wrap the entire path in quotes to guard for it
            format = "\"" + format + "\"";
        }

        return String.format(Locale.ROOT, format, nativeLibsDir, platform, File.pathSeparatorChar, existingLibraryPath);
    }
}
