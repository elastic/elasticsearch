/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * Encodes a version string to a {@link BytesRef} that correctly sorts according to software version precedence rules like
 * the ones described in Semantiv Versioning (https://semver.org/)
 *
 * Version strings are considered to consist of three parts:
 * <ul>
 *  <li> a numeric major.minor.patch part starting the version string (e.g. 1.2.3)
 *  <li> an optional "pre-release" part that starts with a `-` character and can consist of several alpha-numerical sections
 *  separated by dots (e.g. "-alpha.2.3")
 *  <li> an optional "build" part that starts with a `+` character. This will simply be treated as a prefix with no guaranteed ordering,
 *  (although the ordering should be alphabetical in most cases).
 * </ul>
 *
 * The version string is encoded such that the ordering works like the following:
 * <ul>
 *  <li> Major, minor, and patch versions are always compared numerically
 *  <li> pre-release version have lower precedence than a normal version. (e.g 1.0.0-alpha &lt; 1.0.0)
 *  <li> the precedence for pre-release versions with same main version is calculated comparing each dot separated identifier from
 *  left to right. Identifiers consisting of only digits are compared numerically and identifiers with letters or hyphens are compared
 *  lexically in ASCII sort order. Numeric identifiers always have lower precedence than non-numeric identifiers.
 * </ul>
 */
class VersionEncoder {

    public static final byte NUMERIC_MARKER_BYTE = (byte) 0x01;
    public static final byte PRERELESE_SEPARATOR_BYTE = (byte) 0x02;
    public static final byte NO_PRERELESE_SEPARATOR_BYTE = (byte) 0x03;

    private static final char PRERELESE_SEPARATOR = '-';
    private static final char DOT_SEPARATOR = '.';
    private static final char BUILD_SEPARATOR = '+';

    // Regex to test version validity: \d+(\.\d+)*(-[\-\dA-Za-z]+){0,1}(\.[-\dA-Za-z]+)*(\+[\.\-\dA-Za-z]+)?
    // private static Pattern LEGAL_VERSION_PATTERN = Pattern.compile(
    // "\\d+(\\.\\d+)*(-[\\-\\dA-Za-z]+){0,1}(\\.[\\-\\dA-Za-z]+)*(\\+[\\.\\-\\dA-Za-z]+)?"
    // );

    // Regex to test strict Semver Main Version validity:
    // private static Pattern LEGAL_MAIN_VERSION_SEMVER = Pattern.compile("(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)");

    // Regex to test relaxed Semver Main Version validity. Allows for more or less than three main version parts
    private static Pattern LEGAL_MAIN_VERSION_SEMVER = Pattern.compile("(0|[1-9]\\d*)(\\.(0|[1-9]\\d*))*");

    private static Pattern LEGAL_PRERELEASE_VERSION_SEMVER = Pattern.compile(
        "(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))"
    );

    private static Pattern LEGAL_BUILDSUFFIX_SEMVER = Pattern.compile("(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?");

    /**
     * Encodes a version string.
     */
    public static EncodedVersion encodeVersion(String versionString) {
        // System.out.println("encoding: " + versionString);
        VersionParts versionParts = VersionParts.ofVersion(versionString);

        // don't treat non-legal versions further, just mark them as illegal and return
        if (legalVersionString(versionParts) == false) {
            return new EncodedVersion(new BytesRef(versionString), false, true, 0, 0, 0);
        }

        BytesRefBuilder encodedBytes = new BytesRefBuilder();
        Integer[] mainVersionParts = prefixDigitGroupsWithLength(versionParts.mainVersion, encodedBytes);

        if (versionParts.preRelease != null) {
            encodedBytes.append(PRERELESE_SEPARATOR_BYTE);  // versions with pre-release part sort before ones without
            encodedBytes.append((byte) PRERELESE_SEPARATOR);
            String[] preReleaseParts = versionParts.preRelease.substring(1).split("\\.");
            boolean first = true;
            for (String preReleasePart : preReleaseParts) {
                if (first == false) {
                    encodedBytes.append((byte) DOT_SEPARATOR);
                }
                boolean isNumeric = preReleasePart.chars().allMatch(x -> Character.isDigit(x));
                if (isNumeric) {
                    prefixDigitGroupsWithLength(preReleasePart, encodedBytes);
                } else {
                    encodedBytes.append(new BytesRef(preReleasePart));
                }
                first = false;
            }
        } else {
            encodedBytes.append(NO_PRERELESE_SEPARATOR_BYTE);
        }

        if (versionParts.buildSuffix != null) {
            encodedBytes.append(new BytesRef(versionParts.buildSuffix));
        }
        // System.out.println("encoded: " + encodedBytes.toBytesRef());
        return new EncodedVersion(
            encodedBytes.toBytesRef(),
            true,
            versionParts.preRelease != null,
            mainVersionParts[0],
            mainVersionParts[1],
            mainVersionParts[2]
        );
    }

    private static Integer[] prefixDigitGroupsWithLength(String input, BytesRefBuilder result) {
        int pos = 0;
        int mainVersionCounter = 0;
        Integer[] mainVersionComponents = new Integer[3];
        while (pos < input.length()) {
            if (Character.isDigit(input.charAt(pos))) {
                // found beginning of number block, so get its length
                int start = pos;
                BytesRefBuilder number = new BytesRefBuilder();
                while (pos < input.length() && Character.isDigit(input.charAt(pos))) {
                    number.append((byte) input.charAt(pos));
                    pos++;
                }
                int length = pos - start;
                if (length >= 128) {
                    throw new IllegalArgumentException("Groups of digits cannot be longer than 127, but found: " + length);
                }
                result.append(NUMERIC_MARKER_BYTE); // ensure length byte does cause higher sort order comparing to other byte[]
                result.append((byte) (length | 0x80)); // add upper bit to mark as length
                result.append(number);

                // if present, parse out three leftmost version parts
                if (mainVersionCounter < 3) {
                    mainVersionComponents[mainVersionCounter] = Integer.valueOf(number.toBytesRef().utf8ToString());
                    mainVersionCounter++;
                }
            } else {
                result.append((byte) input.charAt(pos));
                pos++;
            }
        }
        return mainVersionComponents;
    }

    public static String decodeVersion(BytesRef version) {
        // System.out.println("decoding: " + version);
        int inputPos = version.offset;
        int resultPos = 0;
        byte[] result = new byte[version.length];
        while (inputPos < version.offset + version.length) {
            byte inputByte = version.bytes[inputPos];
            if (inputByte == NUMERIC_MARKER_BYTE) {
                // need to skip this byte
                inputPos++;
                // this should always be a length encoding, which is skipped by increasing inputPos at the end of the loop
                assert version.bytes[inputPos] < 0;
            } else if (inputByte != PRERELESE_SEPARATOR_BYTE && inputByte != NO_PRERELESE_SEPARATOR_BYTE) {
                result[resultPos] = inputByte;
                resultPos++;
            }
            inputPos++;
        }
        // System.out.println("decoded to: " + new String(result, 0, resultPos));
        return new String(result, 0, resultPos, StandardCharsets.UTF_8);
    }

    private static boolean legalVersionString(VersionParts versionParts) {
        boolean legalMainVersion = LEGAL_MAIN_VERSION_SEMVER.matcher(versionParts.mainVersion).matches();
        boolean legalPreRelease = true;
        if (versionParts.preRelease != null) {
            legalPreRelease = LEGAL_PRERELEASE_VERSION_SEMVER.matcher(versionParts.preRelease).matches();
        }
        boolean legalBuildSuffix = true;
        if (versionParts.buildSuffix != null) {
            legalBuildSuffix = LEGAL_BUILDSUFFIX_SEMVER.matcher(versionParts.buildSuffix).matches();
        }
        return legalMainVersion && legalPreRelease && legalBuildSuffix;
    }

    public static class EncodedVersion {

        public final boolean isLegal;
        public final boolean isPreRelease;
        public final BytesRef bytesRef;
        public final Integer major;
        public final Integer minor;
        public final Integer patch;

        EncodedVersion(BytesRef bytesRef, boolean isLegal, boolean isPreRelease, Integer major, Integer minor, Integer patch) {
            super();
            this.bytesRef = bytesRef;
            this.isLegal = isLegal;
            this.isPreRelease = isPreRelease;
            this.major = major;
            this.minor = minor;
            this.patch = patch;
        }
    }

    private static class VersionParts {
        final String mainVersion;
        final String preRelease;
        final String buildSuffix;

        private VersionParts(String mainVersion, String preRelease, String buildSuffix) {
            this.mainVersion = mainVersion;
            this.preRelease = preRelease;
            this.buildSuffix = buildSuffix;
        }

        private static VersionParts ofVersion(String versionString) {
            String buildSuffix = extractSuffix(versionString, BUILD_SEPARATOR);
            if (buildSuffix != null) {
                versionString = versionString.substring(0, versionString.length() - buildSuffix.length());
            }

            String preRelease = extractSuffix(versionString, PRERELESE_SEPARATOR);
            if (preRelease != null) {
                versionString = versionString.substring(0, versionString.length() - preRelease.length());
            }
            return new VersionParts(versionString, preRelease, buildSuffix);
        }

        private static String extractSuffix(String input, char separator) {
            int start = input.indexOf(separator);
            return start > 0 ? input.substring(start) : null;
        }
    }
}
