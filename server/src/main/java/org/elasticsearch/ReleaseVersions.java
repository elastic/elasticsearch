/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.internal.BuildExtension;
import org.elasticsearch.plugins.ExtensionLoader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.OptionalInt;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ReleaseVersions {

    private static final boolean USES_VERSIONS;

    static {
        USES_VERSIONS = ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class))
            .map(BuildExtension::hasReleaseVersioning)
            .orElse(true);
    }

    private static final Pattern VERSION_LINE = Pattern.compile("(\\d+\\.\\d+\\.\\d+),(\\d+)");

    public static VersionLookup generateVersionsLookup(Class<?> versionContainer) {
        try {
            String versionsFileName = versionContainer.getSimpleName() + ".csv";
            InputStream versionsFile = versionContainer.getResourceAsStream(versionsFileName);
            if (versionsFile == null) {
                throw new FileNotFoundException(Strings.format("Could not find versions file for class [%s]", versionContainer));
            }

            Map<Version, Integer> versions = new HashMap<>();
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
                        var existing = versions.putIfAbsent(version, id);
                        if (existing != null) {
                            throw new IOException(Strings.format("Duplicated version [%s] in [%s]", version, versionsFileName));
                        }
                    } catch (IllegalArgumentException e) {
                        // cannot happen??? regex is wrong...
                        assert false : "Regex allowed non-integer id or incorrect version through: " + e;
                        throw new IOException(Strings.format("Incorrect format for line [%s] in [%s]", line, versionsFileName), e);
                    }
                }
            }

            return USES_VERSIONS ? new Lookup(versions) : new IdLookup(versions);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class IdLookup implements VersionLookup {
        private final Map<Version, Integer> versionToId;

        private IdLookup(Map<Version, Integer> versionToId) {
            this.versionToId = versionToId;
        }

        @Override
        public OptionalInt findId(Version version) {
            Integer id = versionToId.get(version);
            return id != null ? OptionalInt.of(id) : OptionalInt.empty();
        }

        @Override
        public String inferVersion(int id) {
            return Integer.toString(id);
        }
    }

    private static class Lookup implements VersionLookup {
        private final Map<Version, Integer> versionToId;
        private final NavigableMap<Integer, List<Version>> idToVersion;

        private Lookup(Map<Version, Integer> versionToId) {
            this.versionToId = versionToId;

            idToVersion = versionToId.entrySet()
                .stream()
                .collect(
                    Collectors.groupingBy(Map.Entry::getValue, TreeMap::new, Collectors.mapping(Map.Entry::getKey, Collectors.toList()))
                );
            // replace all version lists with the smallest & greatest versions
            idToVersion.replaceAll((k, v) -> {
                if (v.size() == 1) {
                    return List.of(v.get(0));
                } else {
                    v.sort(Comparator.naturalOrder());
                    return List.of(v.get(0), v.get(v.size() - 1));
                }
            });
        }

        @Override
        public OptionalInt findId(Version version) {
            Integer id = versionToId.get(version);
            return id != null ? OptionalInt.of(id) : OptionalInt.empty();
        }

        @Override
        public String inferVersion(int id) {
            List<Version> versionRange = idToVersion.get(id);

            String lowerBound, upperBound;
            if (versionRange != null) {
                lowerBound = versionRange.get(0).toString();
                upperBound = lastItem(versionRange).toString();
            } else {
                // infer the bounds from the surrounding entries
                var lowerRange = idToVersion.lowerEntry(id);
                if (lowerRange != null) {
                    // the next version is just a guess - might be a newer revision, might be a newer minor or major...
                    lowerBound = nextVersion(lastItem(lowerRange.getValue())).toString();
                } else {
                    // a really old version we don't have a record for
                    // assume it's an old version id - we can just return it directly
                    // this will no longer be the case with ES 10 (which won't know about ES v8.x where we introduced separated versions)
                    // maybe keep the release mapping around in the csv file?
                    // SEP for now
                    @UpdateForV9
                    // @UpdateForV10
                    Version oldVersion = Version.fromId(id);
                    return oldVersion.toString();
                }

                var upperRange = idToVersion.higherEntry(id);
                if (upperRange != null) {
                    // too hard to guess what version this id might be for using the next version - just use it directly
                    upperBound = upperRange.getValue().get(0).toString();
                } else {
                    // likely a version created after the last release tagged version - ok
                    upperBound = "snapshot[" + id + "]";
                }
            }

            return lowerBound.equals(upperBound) ? lowerBound : lowerBound + "-" + upperBound;
        }
    }

    private static <T> T lastItem(List<T> list) {
        return list.get(list.size() - 1);
    }

    private static Version nextVersion(Version version) {
        return new Version(version.id + 100);   // +1 to revision
    }
}
