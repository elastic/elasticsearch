/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.versionfield;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Encodes a version string to a {@link BytesRef} while ensuring an ordering that makes sense for
 * software versions.
 *
 * Version strings are considered to consist of three parts in this order:
 * <ul>
 *  <li> a numeric major.minor.patch part starting the version string (e.g. 1.2.3)
 *  <li> an optional "pre-release" part that starts with a `-` character and can consist of several alpha-numerical sections
 *  separated by dots (e.g. "-alpha.2.3")
 *  <li> an optional "build" part that starts with a `+` character. This will simply be treated as a prefix with no guaranteed ordering,
 *  (although the ordering should be alphabetical in most cases).
 * </ul>
 *
 * In the default mode (@link {@link SortMode#SEMVER}, the version string is encoded such that the ordering works like the following:
 * <ul>
 *  <li> Major, minor, and patch versions are always compared numerically
 *  <li> pre-release version have lower precedence than a normal version. (e.g 1.0.0-alpha &lt; 1.0.0)
 *  <li> the precedence for pre-release versions with same main version is calculated comparing each dot separated identifier from
 *  left to right. Identifiers consisting of only digits are compared numerically and identifiers with letters or hyphens are compared
 *  lexically in ASCII sort order. Numeric identifiers always have lower precedence than non-numeric identifiers.
 * </ul>
 *
 * The sorting for the main version part in {@link SortMode#NATURAL} is the same except that in the pre-release part,
 * mixed alpha-numerical identifiers are compared grouping all consecutive digits and treating them as a number with numerical ordering.
 * For example, "alpha2" would sort _before" "alpha11" in this mode.
 */
public class VersionEncoder {

    public static final byte NUMERIC_MARKER_BYTE = (byte) 0x01;
    static final char PRERELESE_SEPARATOR = '-';
    public static final byte PRERELESE_SEPARATOR_BYTE = (byte) 0x02;
    public static final byte NO_PRERELESE_SEPARATOR_BYTE = (byte) 0x03;
    private static final String DOT_SEPARATOR_REGEX = "\\.";
    private static final char DOT_SEPARATOR = '.';
    public static final byte DOT_SEPARATOR_BYTE = (byte) '.';
    private static final char BUILD_SEPARATOR = '+';
    private static final byte BUILD_SEPARATOR_BYTE = (byte) BUILD_SEPARATOR;

    // Regex to test version validity: \d+(\.\d+)*(-[\-\dA-Za-z]+){0,1}(\.[-\dA-Za-z]+)*(\+[\.\-\dA-Za-z]+)?
    private static Pattern LEGAL_VERSION_PATTERN = Pattern.compile(
        "\\d+(\\.\\d+)*(-[\\-\\dA-Za-z]+){0,1}(\\.[\\-\\dA-Za-z]+)*(\\+[\\.\\-\\dA-Za-z]+)?"
    );

    // Regex to test Semver Main Version validity:
    private static Pattern LEGAL_MAIN_VERSION_SEMVER = Pattern.compile("(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)");

    private static Pattern LEGAL_PRERELEASE_VERSION_SEMVER = Pattern.compile(
        "(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))"
    );

    private static Pattern LEGAL_BUILDSUFFIX_SEMVER = Pattern.compile("(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?");

    static boolean strictSemverCheck = false;

    /**
     * Defines how version parts consisting of both alphabetical and numerical characters are ordered
     */
    public enum SortMode {
        /**
         * strict semver precedence treats everything alphabetically, e.g. "rc11" &lt; "rc2"
         */
        SEMVER {

            @Override
            public DocValueFormat docValueFormat() {
                return VERSION_SEMVER;
            }

            @Override
            public String toString() {
                return "semver";
            }
        },
        /**
         * This mode will order mixed strings so that the numeric parts are treated with numeric ordering,
         * e.g. "rc2" &lt; "rc11", "alpha523" &lt; "alpha1234"
         */
        NATURAL {

            @Override
            public DocValueFormat docValueFormat() {
                return VERSION_NUMERIC;
            }

            @Override
            public String toString() {
                return "natural";
            }
        };

        private static DocValueFormat VERSION_SEMVER = new DocValueFormat() {

            @Override
            public String getWriteableName() {
                return "version_semver";
            }

            @Override
            public void writeTo(StreamOutput out) {}

            @Override
            public String format(BytesRef value) {
                return VersionEncoder.decodeVersion(value);
            }

            @Override
            public BytesRef parseBytesRef(String value) {
                return VersionEncoder.encodeVersion(value, SortMode.SEMVER);
            }

            @Override
            public String toString() {
                return getWriteableName();
            }
        };

        private static DocValueFormat VERSION_NUMERIC = new DocValueFormat() {

            @Override
            public String getWriteableName() {
                return "version_numeric";
            }

            @Override
            public void writeTo(StreamOutput out) {}

            @Override
            public String format(BytesRef value) {
                return VersionEncoder.decodeVersion(value);
            }

            @Override
            public BytesRef parseBytesRef(String value) {
                return VersionEncoder.encodeVersion(value, SortMode.NATURAL);
            }

            @Override
            public String toString() {
                return getWriteableName();
            }
        };
        public abstract DocValueFormat docValueFormat();

        public static SortMode fromString(String mode) {
            switch (mode) {
                case "semver":
                    return SEMVER;
                case "natural":
                    return NATURAL;
                default:
                    throw new IllegalArgumentException("Unknown version field mode: " + mode);
            }
        }
    }

    /**
     * Encodes a version string given the given {@link SortMode}.
     * First, the input version string is split into the following parts:
     * <p>
     * mainVersion(-preReleasePart)(+buildId)
     *
     */
    public static BytesRef encodeVersion(String versionString, SortMode mode) {
        return encodeVersion(versionString, mode, true);
    }

    public static BytesRef encodeVersion(String versionString, SortMode mode, boolean validate) {
         System.out.println("encoding: " + versionString);
        // extract "build" suffix starting with "+"
        VersionParts versionParts = VersionParts.ofVersion(versionString);

        if (validate && legalVersionString(versionParts) == false) {
            throw new IllegalArgumentException("Illegal version string: " + versionString);
        }

        // pad all digit groups in main part with numeric marker and length bytes
        BytesRefBuilder encodedVersion = new BytesRefBuilder();
        prefixDigitGroupsWithLength(versionParts.mainVersion, encodedVersion);

        // encode whether version has pre-release parts
        if (versionParts.preRelease != null) {
            encodedVersion.append(PRERELESE_SEPARATOR_BYTE);  // versions with pre-release part sort before ones without
            encodedVersion.append((byte) PRERELESE_SEPARATOR);
            String[] preReleaseParts = versionParts.preRelease.substring(1).split(DOT_SEPARATOR_REGEX);
            boolean first = true;
            for (String preReleasePart : preReleaseParts) {
                if (first == false) {
                    encodedVersion.append(DOT_SEPARATOR_BYTE);
                }
                boolean isNumeric = preReleasePart.chars().allMatch(x -> Character.isDigit(x));
                if (isNumeric) {
                    prefixDigitGroupsWithLength(preReleasePart, encodedVersion);
                } else {
                    if (mode == SortMode.SEMVER) {
                        encodedVersion.append(new BytesRef(preReleasePart));
                    } else {
                        prefixDigitGroupsWithLength(preReleasePart, encodedVersion);
                    }
                }
                first = false;
            }
        } else {
            encodedVersion.append(NO_PRERELESE_SEPARATOR_BYTE);
        }

        // append build part at the end
        if (versionParts.buildSuffix != null) {
            encodedVersion.append(new BytesRef(versionParts.buildSuffix));
        }
        System.out.println("encoded: " + encodedVersion.get());
        return encodedVersion.get();
    }

    private static String extractSuffix(String input, char separator) {
        int start = input.indexOf(separator);
        return start > 0 ? input.substring(start) : null;
    }

    public static void prefixDigitGroupsWithLength(String input, BytesRefBuilder result) {
        int pos = 0;
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
            } else {
                if (input.charAt(pos) == DOT_SEPARATOR) {
                    result.append(DOT_SEPARATOR_BYTE);
                } else {
                    result.append((byte) input.charAt(pos));
                }
                pos++;
            }
        }
    }

    public static String decodeVersion(BytesRef version) {
        // System.out.println("decoding: " + version);
        int pos = 0;
        StringBuilder sb = new StringBuilder();
        while (pos < version.length && version.bytes[pos] != BUILD_SEPARATOR_BYTE) {
            pos++;
        }
        sb.append(decodeVersionString(version.bytes, 0, pos));

        // add build part if present
        if (pos < version.length && version.bytes[pos] == BUILD_SEPARATOR_BYTE) {
            sb.append(new BytesRef(version.bytes, pos, version.length - pos).utf8ToString());
            // sb.append(new BytesRef(Arrays.copyOfRange(version.bytes, pos, version.length)).utf8ToString());
        }
        return sb.toString();
    }

    private static char[] decodeVersionString(byte[] input, int startPos, int endPos) {
        int inputPos = startPos;
        int resultPos = 0;
        char[] result = new char[input.length];
        while (inputPos < endPos) {
            byte inputByte = input[inputPos];
            if (inputByte >= 0x30 && ((inputByte & 0x80) == 0)) {
                result[resultPos] = (char) inputByte;
                resultPos++;
            } else if (inputByte == DOT_SEPARATOR_BYTE) {
                result[resultPos] = DOT_SEPARATOR;
                resultPos++;
            } else if (inputByte == PRERELESE_SEPARATOR) {
                result[resultPos] = PRERELESE_SEPARATOR;
                resultPos++;
            } else if (inputByte == BUILD_SEPARATOR_BYTE) {
                result[resultPos] = BUILD_SEPARATOR;
                resultPos++;
            }
            inputPos++;
        }
        return Arrays.copyOf(result, resultPos);
    }

    static boolean legalVersionString(VersionParts versionParts) {
        boolean basic = LEGAL_VERSION_PATTERN.matcher(versionParts.all).matches();
        if (strictSemverCheck) {
            boolean mainVersionMatches = LEGAL_MAIN_VERSION_SEMVER.matcher(versionParts.mainVersion).matches();
            boolean preReleaseMatches = versionParts.preRelease == null
                ? true
                : LEGAL_PRERELEASE_VERSION_SEMVER.matcher(versionParts.preRelease).matches();
            boolean buildSuffixMatches = versionParts.buildSuffix == null
                ? true
                : LEGAL_BUILDSUFFIX_SEMVER.matcher(versionParts.buildSuffix).matches();
            return mainVersionMatches && preReleaseMatches && buildSuffixMatches;
        }
        return basic;
    }

    static class VersionParts {
        final String all;
        final String mainVersion;
        final String preRelease;
        final String buildSuffix;

        private VersionParts(String all, String mainVersion, String preRelease, String buildSuffix) {
            this.all = all;
            this.mainVersion = mainVersion;
            this.preRelease = preRelease;
            this.buildSuffix = buildSuffix;
        }

        static VersionParts ofVersion(String versionString) {
            String versionStringOriginal = versionString;
            String buildSuffix = extractSuffix(versionString, BUILD_SEPARATOR);
            if (buildSuffix != null) {
                versionString = versionString.substring(0, versionString.length() - buildSuffix.length());
            }

            // extract "pre-release" suffix starting with "-"
            String preRelease = extractSuffix(versionString, PRERELESE_SEPARATOR);
            if (preRelease != null) {
                versionString = versionString.substring(0, versionString.length() - preRelease.length());
            }
            return new VersionParts(versionStringOriginal, versionString, preRelease, buildSuffix);
        }

    }
}
