/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.internal.BuildExtension;
import org.elasticsearch.plugins.ExtensionLoader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.ServiceLoader;
import java.util.TreeMap;
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

    public static IntFunction<String> generateVersionsLookup(Class<?> versionContainer, int current) {
        if (USES_VERSIONS == false) return Integer::toString;

        try {
            String versionsFileName = versionContainer.getSimpleName() + ".csv";
            InputStream versionsFile = versionContainer.getResourceAsStream(versionsFileName);
            if (versionsFile == null) {
                throw new FileNotFoundException(Strings.format("Could not find versions file for class [%s]", versionContainer));
            }

            NavigableMap<Integer, List<Version>> versions = new TreeMap<>();
            // add the current version id, which won't be in the csv
            versions.computeIfAbsent(current, k -> new ArrayList<>()).add(Version.CURRENT);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(versionsFile, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    var matcher = VERSION_LINE.matcher(line);
                    if (matcher.matches() == false) {
                        throw new IOException(Strings.format("Incorrect format for line [%s] in [%s]", line, versionsFileName));
                    }
                    try {
                        Integer id = Integer.valueOf(matcher.group(2));
                        Version version = Version.fromString(matcher.group(1));
                        versions.computeIfAbsent(id, k -> new ArrayList<>()).add(version);
                    } catch (IllegalArgumentException e) {
                        // cannot happen??? regex is wrong...
                        assert false : "Regex allowed non-integer id or incorrect version through: " + e;
                        throw new IOException(Strings.format("Incorrect format for line [%s] in [%s]", line, versionsFileName), e);
                    }
                }
            }

            // replace all version lists with the smallest & greatest versions
            versions.replaceAll((k, v) -> {
                if (v.size() == 1) {
                    return List.of(v.getFirst());
                } else {
                    v.sort(Comparator.naturalOrder());
                    return List.of(v.getFirst(), v.getLast());
                }
            });

            return lookupFunction(versions);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static IntFunction<String> lookupFunction(NavigableMap<Integer, List<Version>> versions) {
        assert versions.values().stream().allMatch(vs -> vs.size() == 1 || vs.size() == 2)
            : "Version ranges have not been properly processed: " + versions;

        return id -> {
            List<Version> versionRange = versions.get(id);

            String lowerBound, upperBound;
            if (versionRange != null) {
                lowerBound = versionRange.getFirst().toString();
                upperBound = versionRange.getLast().toString();
            } else {
                // infer the bounds from the surrounding entries
                var lowerRange = versions.lowerEntry(id);
                if (lowerRange != null) {
                    // the next version is just a guess - might be a newer revision, might be a newer minor or major...
                    lowerBound = nextVersion(lowerRange.getValue().getLast()).toString();
                } else {
                    // a really old version we don't have a record for
                    // assume it's an old version id - we can just return it directly
                    // this will no longer be the case with ES 10 (which won't know about ES v8.x where we introduced separated versions)
                    // maybe keep the release mapping around in the csv file?
                    // SEP for now
                    @UpdateForV10(owner = UpdateForV10.Owner.CORE_INFRA)
                    Version oldVersion = Version.fromId(id);
                    return oldVersion.toString();
                }

                var upperRange = versions.higherEntry(id);
                if (upperRange != null) {
                    // too hard to guess what version this id might be for using the next version - just use it directly
                    upperBound = upperRange.getValue().getFirst().toString();
                } else {
                    // a newer version than all we know about? Can't map it...
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
