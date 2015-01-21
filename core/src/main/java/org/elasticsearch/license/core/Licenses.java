/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Utility class for operating on a collection of {@link License}s.
 * Provides serialization/deserialization methods and reduce
 * operations
 */
public final class Licenses {

    /**
     * XContent param name to deserialize license(s) with
     * an additional <code>status</code> field, indicating whether a
     * particular license is 'active' or 'expired' and no signature
     * and in a human readable format
     */
    public static final String REST_VIEW_MODE = "rest_view";

    /**
     * XContent param name to deserialize license(s) with
     * no signature
     */
    public static final String LICENSE_SPEC_VIEW_MODE = "license_spec_view";

    private final static class Fields {
        static final String LICENSES = "licenses";
    }

    private final static class XFields {
        static final XContentBuilderString LICENSES = new XContentBuilderString(Fields.LICENSES);
    }

    private Licenses() {}

    public static void toXContent(Collection<License> licenses, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray(XFields.LICENSES);
        for (License license : licenses) {
            license.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
    }

    public static List<License> fromSource(String content) throws IOException {
        return fromSource(content.getBytes(StandardCharsets.UTF_8), true);
    }

    public static List<License> fromSource(byte[] bytes) throws IOException {
        return fromXContent(XContentFactory.xContent(bytes).createParser(bytes), true);
    }

    public static List<License> fromSource(byte[] bytes, boolean verify) throws IOException {
        return fromXContent(XContentFactory.xContent(bytes).createParser(bytes), verify);
    }

    public static List<License> fromXContent(XContentParser parser, boolean verify) throws IOException {
        List<License> licenses = new ArrayList<>();
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                if (Fields.LICENSES.equals(currentFieldName)) {
                    if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            License.Builder builder = License.builder().fromXContent(parser);
                            if (verify) {
                                builder.validate();
                            }
                            licenses.add(builder.build());
                        }
                    } else {
                        throw new ElasticsearchParseException("failed to parse licenses expected an array of licenses");
                    }
                }
                // Ignore all other fields - might be created with new version
            } else {
                throw new ElasticsearchParseException("failed to parse licenses expected field");
            }
        } else {
            throw new ElasticsearchParseException("failed to parse licenses expected start object");
        }
        return licenses;
    }

    public static List<License> readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<License> licenses = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            licenses.add(License.readLicense(in));
        }
        return licenses;
    }

    public static void writeTo(List<License> licenses, StreamOutput out) throws IOException {
        out.writeVInt(licenses.size());
        for (License license : licenses) {
            license.writeTo(out);
        }
    }

    /**
     * Given a set of {@link License}s, reduces the set to one license per feature.
     * Uses {@link #putIfAppropriate(java.util.Map, License)} to reduce.
     *
     * @param licensesSet a set of licenses to be reduced
     * @return a map of (feature, license)
     */
    public static ImmutableMap<String, License> reduceAndMap(Set<License> licensesSet) {
        Map<String, License> map = new HashMap<>(licensesSet.size());
        for (License license : licensesSet) {
            putIfAppropriate(map, license);
        }
        return ImmutableMap.copyOf(map);
    }

    /**
     * Adds or updates the <code>license</code> to <code>licenseMap</code> if the <code>license</code>
     * has not expired already and if the <code>license</code> has a later expiry date from any
     * existing licenses in <code>licenseMap</code> for the same feature
     *
     * @param licenseMap a map of (feature, license)
     * @param license a new license to be added to <code>licenseMap</code>
     */
    private static void putIfAppropriate(Map<String, License> licenseMap, License license) {
        final String featureType = license.feature();
        if (licenseMap.containsKey(featureType)) {
            final License previousLicense = licenseMap.get(featureType);
            if (license.expiryDate() > previousLicense.expiryDate()) {
                licenseMap.put(featureType, license);
            }
        } else {
            licenseMap.put(featureType, license);
        }
    }
}
