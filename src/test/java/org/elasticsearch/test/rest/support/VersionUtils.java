/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test.rest.support;

public final class VersionUtils {

    private VersionUtils() {

    }

    /**
     * Parses an elasticsearch version string into an int array with an element per part
     * e.g. 0.90.7 => [0,90,7]
     */
    public static int[] parseVersionNumber(String version) {
        String[] split = version.split("\\.");
        //we only take the first 3 parts if there are more, but less is ok too (e.g. 999)
        int length = Math.min(3, split.length);
        int[] versionNumber = new int[length];
        for (int i = 0; i < length; i++) {
            try {
                versionNumber[i] = Integer.valueOf(split[i]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("version is not a number", e);
            }

        }
        return versionNumber;
    }

    /**
     * Compares the skip version read from a test fragment with the elasticsearch version
     * the tests are running against and determines whether the test fragment needs to be skipped
     */
    public static boolean skipCurrentVersion(String skipVersion, String currentVersion) {
        int[] currentVersionNumber = parseVersionNumber(currentVersion);

        String[] skipVersions = skipVersion.split("-");
        if (skipVersions.length > 2) {
            throw new IllegalArgumentException("too many skip versions found");
        }

        String skipVersionLowerBound = skipVersions[0].trim();
        String skipVersionUpperBound = skipVersions[1].trim();

        int[] skipVersionLowerBoundNumber = parseVersionNumber(skipVersionLowerBound);
        int[] skipVersionUpperBoundNumber = parseVersionNumber(skipVersionUpperBound);

        int length = Math.min(skipVersionLowerBoundNumber.length, currentVersionNumber.length);
        for (int i = 0; i < length; i++) {
            if (currentVersionNumber[i] < skipVersionLowerBoundNumber[i]) {
                return false;
            }
            if (currentVersionNumber[i] > skipVersionLowerBoundNumber[i]) {
                break;
            }
        }

        length = Math.min(skipVersionUpperBoundNumber.length, currentVersionNumber.length);
        for (int i = 0; i < length; i++) {
            if (currentVersionNumber[i] > skipVersionUpperBoundNumber[i]) {
                return false;
            }
            if (currentVersionNumber[i] < skipVersionUpperBoundNumber[i]) {
                break;
            }
        }

        return true;
    }
}
