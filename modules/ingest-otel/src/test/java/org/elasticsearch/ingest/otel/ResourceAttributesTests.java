/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.otel;

import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ResourceAttributesTests extends ESTestCase {

    public void testResourceAttributesWebCrawler() {
        Set<String> resourceAttributes = OTelSemConvWebCrawler.collectOTelSemConvResourceAttributes();
        System.out.println("Resource Attributes: " + resourceAttributes.size());
        for (String attribute : resourceAttributes) {
            System.out.println(attribute);
        }
    }

    public void testEcsToOTelAttributeNames() {
        Map<String, String> attributes = EcsFieldsDiscoverer.getInstance().getEcsToOTelAttributeNames();
        System.out.println("ECS to OTel attribute mappings: " + attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            System.out.println(entry.getKey() + " --> " + entry.getValue());
        }
    }

    public void testAttributesSetUpToDate() {
        Map<String, String> ecsToOTelAttributeNames = EcsFieldsDiscoverer.getInstance().getEcsToOTelAttributeNames();
        Set<String> otelResourceAttributes = OTelSemConvWebCrawler.collectOTelSemConvResourceAttributes();
        Set<String> latestEcsOTelResourceAttributes = new HashSet<>();
        ecsToOTelAttributeNames.forEach((ecsAttributeName, otelAttributeName) -> {
            if (otelResourceAttributes.contains(otelAttributeName)) {
                latestEcsOTelResourceAttributes.add(ecsAttributeName);
            }
        });
        latestEcsOTelResourceAttributes.addAll(EcsFieldsDiscoverer.getInstance().getEcsResourceFields());
        if (latestEcsOTelResourceAttributes.equals(EcsOTelResourceAttributes.LATEST)) {
            System.out.println("ECS to OTel resource attributes are up to date.");
        } else {
            System.out.println("ECS to OTel resource attributes are not up to date.");
            // find and print the diff
            Set<String> addedAttributes = new HashSet<>(latestEcsOTelResourceAttributes);
            addedAttributes.removeAll(EcsOTelResourceAttributes.LATEST);
            if (addedAttributes.isEmpty() == false) {
                System.out.println();
                System.out.println("The current resource attributes set doesn't contain the following attributes:");
                System.out.println("-----------------------------------------------------------------------------");
                for (String attribute : addedAttributes) {
                    System.out.println(attribute);
                }
                System.out.println("-----------------------------------------------------------------------------");
                System.out.println();
            }
            Set<String> removedAttributes = new HashSet<>(EcsOTelResourceAttributes.LATEST);
            removedAttributes.removeAll(latestEcsOTelResourceAttributes);
            if (removedAttributes.isEmpty() == false) {
                System.out.println();
                System.out.println("The following attributes are no longer considered resource attributes:");
                System.out.println("----------------------------------------------------------------------");
                for (String attribute : removedAttributes) {
                    System.out.println(attribute);
                }
                System.out.println("----------------------------------------------------------------------");
                System.out.println();
            }
            System.out.println("Consider updating EcsOTelResourceAttributes accordingly");
            System.out.println();
            fail("ECS to OTel resource attributes are not up to date");
        }
    }
}
