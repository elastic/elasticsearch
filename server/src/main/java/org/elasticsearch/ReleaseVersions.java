/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.internal.BuildExtension;
import org.elasticsearch.plugins.ExtensionLoader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.IntFunction;
import java.util.regex.Pattern;

public class ReleaseVersions {

    private static final boolean USES_VERSIONS;

    static {
        USES_VERSIONS = ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class))
            .map(BuildExtension::hasReleaseVersioning)
            .orElse(true);
    }

    private static final Pattern VERSION_LINE = Pattern.compile("(\\d+\\.\\d+\\.\\d+),(\\d+)");

    public static IntFunction<String> generateVersionsLookup(Class<?> versionContainer) {
        if (USES_VERSIONS == false) return Integer::toString;

        try {
            String versionsFileName = versionContainer.getSimpleName() + ".csv";
            URL versionsFile = versionContainer.getResource(versionsFileName);
            if (versionsFile == null) {
                throw new FileNotFoundException(Strings.format("Could not find versions file for class [%s]", versionContainer));
            }

            try (
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(versionContainer.getResourceAsStream(versionsFileName), StandardCharsets.UTF_8)
                )
            ) {
                NavigableMap<Integer, NavigableSet<Version>> versions = new TreeMap<>();
                String line;
                while ((line = reader.readLine()) != null) {
                    var matcher = VERSION_LINE.matcher(line);
                    if (matcher.matches() == false) {
                        throw new IOException(Strings.format("Incorrect format for line [%s] in [%s]", line, versionsFile));
                    }
                    try {
                        Integer id = Integer.valueOf(matcher.group(2));
                        Version version = Version.fromString(matcher.group(1));
                        versions.computeIfAbsent(id, k -> new TreeSet<>()).add(version);
                    } catch (IllegalArgumentException e) {
                        // cannot happen??? regex is wrong...
                        assert false : "Regex allowed non-integer id or incorrect version through: " + e;
                        throw new IOException(Strings.format("Incorrect format for line [%s] in [%s]", line, versionsFile), e);
                    }
                }

                return lookupFunction(versions);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static IntFunction<String> lookupFunction(NavigableMap<Integer, NavigableSet<Version>> versions) {
        return id -> {
            NavigableSet<Version> versionRange = versions.get(id);

            String lowerBound, upperBound;
            if (versionRange != null) {
                lowerBound = versionRange.first().toString();
                upperBound = versionRange.last().toString();
            } else {
                // infer the bounds from the surrounding entries
                var lowerRange = versions.lowerEntry(id);
                if (lowerRange != null) {
                    // the next version is just a guess - might be a newer revision, might be a newer minor or major...
                    lowerBound = nextVersion(lowerRange.getValue().last()).toString();
                } else {
                    // we know about all preceding versions - how can this version be less than everything else we know about???
                    assert false : "Could not find preceding version for id " + id;
                    lowerBound = "[" + id + "]";
                }

                var upperRange = versions.higherEntry(id);
                if (upperRange != null) {
                    // too hard to guess what version this id might be for using the next version - just use it directly
                    upperBound = upperRange.getValue().first().toString();
                } else {
                    // likely a version used after this code was released - ok
                    upperBound = "[" + id + "]";
                }
            }

            return lowerBound.equals(upperBound) ? lowerBound : lowerBound + "-" + upperBound;
        };
    }

    private static Version nextVersion(Version version) {
        return new Version(version.id + 100);   // +1 to revision
    }
}
