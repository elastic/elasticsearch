/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.core.DateUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.LicensesCharset;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import static org.elasticsearch.license.core.ESLicense.SubscriptionType;
import static org.elasticsearch.license.core.ESLicense.Type;

public class LicenseSpecs {

    private static ESLicense fromXContent(Map<String, Object> map) throws IOException, ParseException {
        ESLicense.Builder builder = new ESLicense.Builder()
                .type(Type.fromString((String) map.get("type")))
                .subscriptionType(SubscriptionType.fromString((String) map.get("subscription_type")))
                .feature((String) map.get("feature"))
                .maxNodes((int) map.get("max_nodes"))
                .issuedTo((String) map.get("issued_to"))
                .issuer((String) map.get("issuer"));

        String uid = (String) map.get("uid");
        if (uid == null) {
            builder.uid(UUID.randomUUID().toString());
        }

        String issueDate = (String) map.get("issue_date");
        builder.issueDate(DateUtils.longFromDateString(issueDate));

        String expiryDate = (String) map.get("expiry_date");
        builder.expiryDate(DateUtils.longExpiryDateFromString(expiryDate));

        return builder.build();
    }

    public static Set<ESLicense> fromSource(String content) throws IOException, ParseException {
        return fromSource(content.getBytes(LicensesCharset.UTF_8));
    }

    public static Set<ESLicense> fromSource(byte[] bytes) throws IOException, ParseException {
        return fromXContents(XContentFactory.xContent(bytes).createParser(bytes));
    }

    private static Set<ESLicense> fromXContents(XContentParser parser) throws IOException, ParseException {
        Set<ESLicense> licenseSpecs = new HashSet<>();
        final Map<String, Object> licenseSpecMap = parser.mapAndClose();
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> licenseSpecDefinitions = (ArrayList<Map<String, Object>>)licenseSpecMap.get("licenses");
        for (Map<String, Object> licenseSpecDef : licenseSpecDefinitions) {
            final ESLicense licenseSpec = fromXContent(licenseSpecDef);
            licenseSpecs.add(licenseSpec);
        }
        return licenseSpecs;
    }
}

