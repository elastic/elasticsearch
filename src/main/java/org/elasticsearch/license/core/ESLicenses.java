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
import java.util.*;

public class ESLicenses {

    final static class Fields {
        static final String LICENSES = "licenses";
    }

    private final static class XFields {
        static final XContentBuilderString LICENSES = new XContentBuilderString(Fields.LICENSES);
    }

    public static void toXContent(Collection<ESLicense> licenses, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray(XFields.LICENSES);
        for (ESLicense license : licenses) {
            license.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
    }

    public static List<ESLicense> fromSource(String content) throws IOException {
        return fromSource(content.getBytes(LicensesCharset.UTF_8));
    }

    public static List<ESLicense> fromSource(byte[] bytes) throws IOException {
        return fromXContent(XContentFactory.xContent(bytes).createParser(bytes));
    }

    public static List<ESLicense> fromXContent(XContentParser parser) throws IOException {
        List<ESLicense> esLicenses = new ArrayList<>();
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            if (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                if (Fields.LICENSES.equals(currentFieldName)) {
                    if (parser.nextToken() == XContentParser.Token.START_ARRAY) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            esLicenses.add(ESLicense.fromXContent(parser));
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
        return esLicenses;
    }

    public static List<ESLicense> readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<ESLicense> esLicenses = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            esLicenses.add(ESLicense.readESLicense(in));
        }
        return esLicenses;
    }

    public static void writeTo(List<ESLicense> esLicenses, StreamOutput out) throws IOException {
        out.writeVInt(esLicenses.size());
        for (ESLicense license : esLicenses) {
            license.writeTo(out);
        }
    }

    public static ImmutableMap<String, ESLicense> reduceAndMap(Set<ESLicense> esLicensesSet) {
        Map<String, ESLicense> map = new HashMap<>(esLicensesSet.size());
        for (ESLicense license : esLicensesSet) {
            putIfAppropriate(map, license);
        }
        return ImmutableMap.copyOf(map);
    }

    private static void putIfAppropriate(Map<String, ESLicense> licenseMap, ESLicense license) {
        final String featureType = license.feature();
        if (licenseMap.containsKey(featureType)) {
            final ESLicense previousLicense = licenseMap.get(featureType);
            if (license.expiryDate() > previousLicense.expiryDate()) {
                licenseMap.put(featureType, license);
            }
        } else if (license.expiryDate() > System.currentTimeMillis()) {
            licenseMap.put(featureType, license);
        }
    }
}
