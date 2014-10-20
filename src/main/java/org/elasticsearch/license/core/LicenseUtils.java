/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

public class LicenseUtils {

    public static void dumpLicenseAsJson(ESLicenses esLicenses, OutputStream out) throws IOException {
        JsonGenerator generator = new JsonFactory().createJsonGenerator(out);
        //generator.useDefaultPrettyPrinter();

        generator.writeStartObject();
        {
            generator.writeArrayFieldStart("licenses");
            {
                for (ESLicenses.ESLicense esLicense : esLicenses) {
                    generator.writeStartObject();
                    {
                        generator.writeStringField("uid", esLicense.uid());
                        generator.writeStringField("type", esLicense.type().string());
                        generator.writeStringField("subscription_type", esLicense.subscriptionType().string());
                        generator.writeStringField("issued_to", esLicense.issuedTo());
                        generator.writeStringField("issue_date", DateUtils.dateStringFromLongDate(esLicense.issueDate()));
                        generator.writeStringField("expiry_date", DateUtils.dateStringFromLongDate(esLicense.expiryDate()));
                        generator.writeStringField("feature", esLicense.feature());
                        generator.writeNumberField("max_nodes", esLicense.maxNodes());
                        generator.writeStringField("signature", esLicense.signature());
                    }
                    generator.writeEndObject();
                }
            }
            generator.writeEndArray();
        }
        generator.writeEndObject();
        generator.flush();
    }

    public static Set<ESLicenses> readLicensesFromFiles(Set<File> licenseFiles) throws IOException {
        Set<ESLicenses> esLicensesSet = new HashSet<>();
        for (File licenseFile : licenseFiles) {
            esLicensesSet.add(LicenseUtils.readLicenseFile(licenseFile));
        }
        return esLicensesSet;
    }


    public static Set<ESLicenses> readLicensesFromDirectory(File licenseDirectory) throws IOException {
        Set<ESLicenses> esLicensesSet = new HashSet<>();
        if (!licenseDirectory.exists()) {
            throw new IllegalArgumentException(licenseDirectory.getAbsolutePath() + " does not exist!");
        }
        if (licenseDirectory.isDirectory()) {
            for (File licenseFile : FileUtils.listFiles(licenseDirectory, new String[]{"json"}, false)) {
                esLicensesSet.add(readLicenseFile(licenseFile));
            }
        } else if (licenseDirectory.isFile()) {
            esLicensesSet.add(readLicenseFile(licenseDirectory));
        } else {
            throw new IllegalArgumentException(licenseDirectory.getAbsolutePath() + "is not a file or a directory");
        }
        return esLicensesSet;
    }

    public static ESLicenses readLicenseFile(File licenseFile) throws IOException {
        try (FileInputStream fileInputStream = new FileInputStream(licenseFile)) {
            return readLicenseFromInputStream(fileInputStream);
        }
    }

    public static ESLicenses readLicenseFromInputStream(InputStream inputStream) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(inputStream);
        return extractLicenseFromJson(jsonNode);
    }

    public static ESLicenses readLicensesFromString(String licensesString) throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(licensesString);
        return extractLicenseFromJson(jsonNode);
    }

    private static ESLicenses extractLicenseFromJson(final JsonNode jsonNode) {
        final LicenseBuilders.LicensesBuilder licensesBuilder = LicenseBuilders.licensesBuilder();
        JsonNode licensesNode = jsonNode.get("licenses");
        if (licensesNode.isArray()) {
            for (JsonNode licenseNode : licensesNode) {
                licensesBuilder.license(LicenseBuilders.licenseBuilder(false)
                        .uid(getValueAsString(licenseNode, "uid", true))
                        .issuedTo(getValueAsString(licenseNode, "issued_to"))
                        .issuer(getValueAsString(licenseNode, "issuer", true))
                        .issueDate(getValueAsDate(licenseNode, "issue_date"))
                        .type(ESLicenses.Type.fromString(getValueAsString(licenseNode, "type")))
                        .subscriptionType(ESLicenses.SubscriptionType.fromString(getValueAsString(licenseNode, "subscription_type")))
                        .feature(getValueAsString(licenseNode, "feature"))
                        .expiryDate(getValueAsExpiryDate(licenseNode, "expiry_date"))
                        .maxNodes(getValueAsInt(licenseNode, "max_nodes"))
                        .signature(getValueAsString(licenseNode, "signature", true))
                        .build());
            }
        } else {
            throw new IllegalStateException("'licenses' field is not an array");
        }
        return licensesBuilder.build();

    }

    private static int getValueAsInt(final JsonNode jsonNode, String field) {
        JsonNode node = getFieldNode(jsonNode, field, false);
        assert node.isNumber();
        return node.getValueAsInt();
    }

    private static String getValueAsString(final JsonNode jsonNode, String field) {
        return getValueAsString(jsonNode, field, false);
    }

    private static String getValueAsString(final JsonNode jsonNode, String field, boolean optional) {
        JsonNode node = getFieldNode(jsonNode, field, optional);
        assert node != null || optional;
        if (node == null) {
            return null;
        }
        assert !node.isObject();
        return node.getTextValue();
    }

    private static long getValueAsDate(final JsonNode jsonNode, String field) {
        JsonNode node = getFieldNode(jsonNode, field, false);
        assert !node.isObject();
        final String value = node.getTextValue();
        try {
            return DateUtils.longFromDateString(value);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static long getValueAsExpiryDate(final JsonNode jsonNode, String field) {
        long actualDate = getValueAsDate(jsonNode, field);
        return DateUtils.longExpiryDateFromDate(actualDate);
    }


    private static JsonNode getFieldNode(final JsonNode jsonNode, String field, boolean optional) {
        JsonNode node = jsonNode.get(field);
        if (node == null && !optional) {
            throw new IllegalArgumentException("field ['" + field + "'] is missing");
        }
        return node;
    }


    public static void printLicense(ESLicenses licenses) {
        for (ESLicenses.ESLicense license : licenses) {
            System.out.println("===");
            printValue("  uid", license.uid());
            printValue("  type", license.type().string());
            printValue("  subscription_type", license.subscriptionType().string());
            printValue("  issueDate", DateUtils.dateStringFromLongDate(license.issueDate()));
            printValue("  issuedTo", license.issuedTo());
            printValue("  feature", license.feature());
            printValue("  maxNodes", license.maxNodes());
            printValue("  expiryDate", DateUtils.dateStringFromLongDate(license.expiryDate()));
            printValue("  signature", license.signature());
            System.out.println("===");
        }

    }

    private static void printValue(String name, Object value) {
        System.out.println(name + " : " + value);
    }
}
