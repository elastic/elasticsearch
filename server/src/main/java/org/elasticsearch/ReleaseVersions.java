/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.internal.BuildExtension;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ExtensionLoader;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class ReleaseVersions {

    private static final Logger Log = LogManager.getLogger(ReleaseVersions.class);

    private static final boolean USES_VERSIONS;

    static {
        USES_VERSIONS = ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class))
            .map(BuildExtension::hasReleaseVersioning)
            .orElse(true);
    }

    private static final Map<Class<?>, NavigableMap<Integer, String>> RESOLVED_VERSIONS = new ConcurrentHashMap<>();

    private static final Pattern VERSION_LINE = Pattern.compile("(\\d+\\.\\d+\\.\\d+),(\\d+)(\\h*#.*)?");

    static NavigableMap<Integer, String> readVersionsFile(Class<?> versionContainer) {
        String versionsFileName = versionContainer.getSimpleName() + ".csv";
        URL versionsFile = versionContainer.getResource(versionsFileName);
        if (versionsFile == null) {
            Log.error("Could not find versions file for class [{}]", versionContainer);
            return Collections.emptyNavigableMap();
        }

        try (
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(versionContainer.getResourceAsStream(versionsFileName), StandardCharsets.UTF_8)
            )
        ) {
            NavigableMap<Integer, String> versions = new TreeMap<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#")) continue; // a comment

                var matcher = VERSION_LINE.matcher(line);
                if (matcher.matches() == false) {
                    Log.warn("Incorrect format for line [{}] in [{}]", line, versionsFile);
                    continue;
                }
                try {
                    Integer id = Integer.valueOf(matcher.group(2));
                    String version = matcher.group(1);
                    String existing = versions.putIfAbsent(id, version);
                    if (existing != null) {
                        Log.warn("Duplicate id [{}] for versions [{}, {}] in [{}]", id, existing, version, versionsFile);
                    }
                } catch (NumberFormatException e) {
                    // cannot happen??? regex is wrong...
                    assert false : "Regex allowed non-integer id through: " + e;
                    Log.warn("Incorrect format for line [{}] in [{}]", line, versionsFile, e);
                }
            }

            return Collections.unmodifiableNavigableMap(versions);
        } catch (Exception e) {
            Log.error("Could not read versions file [{}]", versionsFile, e);
            return Collections.emptyNavigableMap();
        }
    }

    private static NavigableMap<Integer, String> findVersionsMap(Class<?> versionContainer) {
        return RESOLVED_VERSIONS.computeIfAbsent(versionContainer, ReleaseVersions::readVersionsFile);
    }

    public static String findReleaseVersion(Class<?> versionContainer, int id) {
        if (USES_VERSIONS == false) return Integer.toString(id);

        NavigableMap<Integer, String> versions = findVersionsMap(versionContainer);
        String exactVersion = versions.get(id);
        if (exactVersion != null) {
            return exactVersion;
        }

        // go for a range
        if (versions.isEmpty()) {
            // uh oh, something went wrong reading the file
            return "<" + id + ">";
        }

        StringBuilder range = new StringBuilder();

        var lowerRange = versions.lowerEntry(id);
        if (lowerRange != null) {
            range.append(lowerRange.getValue());
        } else {
            range.append('<').append(id).append('>');
        }

        range.append("-");

        var upperRange = versions.higherEntry(id);
        if (upperRange != null) {
            range.append(upperRange.getValue());
        } else {
            range.append('<').append(id).append('>');
        }

        return range.toString();
    }
}
