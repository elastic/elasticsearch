/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TransportVersionUtils {
    public static final String LATEST_SUFFIX = "-LATEST.json";
    public static final String JSON_SUFFIX = ".json";

    public static class TransportVersionSetData implements Serializable {
        public String name;
        public List<Integer> ids;

        public TransportVersionSetData(String name, List<Integer> ids) {
            this.name = name;
            this.ids = ids;
        }

        public TransportVersionSetData(String name) {
            this(name, new ArrayList<>());
        }

        public void writeToDataDir(File tvDataDir) {
            TransportVersionUtils.writeTVSetData(tvDataDir, name, this);
        }
    }

    public static void writeTVSetData(File tvDataDir, String filename, TransportVersionSetData versionSetData) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            File tvSetFile = tvDataDir.toPath().resolve(filename + JSON_SUFFIX).toFile();
            mapper.writeValue(tvSetFile, versionSetData);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write the TransportVersionSet data file: " + tvDataDir.getAbsolutePath(), e);
        }
    }

    public static TransportVersionSetData getLatestTVSetData(File tvDataDir, String majorMinor) {
        return getTVSetData(tvDataDir.toPath().resolve(majorMinor + LATEST_SUFFIX));

    }

    public static TransportVersionSetData getTVSetData(File tvDataDir, String tvSetNameField) {
        return getTVSetData(tvDataDir.toPath().resolve(tvSetNameField + JSON_SUFFIX));
    }

    /**
     * Returns the TransportVersionSetData read from the file at the specified path, null if no file exists.
     */
    public static TransportVersionSetData getTVSetData(Path path) {
        File tvSetDataFile = path.toFile();
        if (tvSetDataFile.exists() == false) {
            return null;
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(tvSetDataFile, TransportVersionSetData.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read the TransportVersionSet data file: " + tvSetDataFile.getAbsolutePath(), e);
        }
    }

    public static String formatLatestTVSetFilename(int major, int minor) {
        return formatLatestTVSetFilename(major + "." + minor);
    }

    public static String formatLatestTVSetFilename(String majorMinor) {
        return majorMinor + LATEST_SUFFIX;
    }
}
