/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.otel;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;
import org.junit.Ignore;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <b>DISABLED BY DEFAULT!</b><br><br>
 * These tests are not meant for CI, but rather to be run manually to check whether the static {@link EcsOTelResourceAttributes resource
 * attributes set} is up to date with the latest ECS and/or OpenTelemetry Semantic Conventions.
 * We may add them to CI in the future, but as such that run periodically (nightly/weekly) and used to notify whenever the resource
 * attributes set is not up to date.
 */
@SuppressForbidden(reason = "Disabled temporarily until we set up the periodic CI pipeline")
@Ignore
public class ResourceAttributesTests extends ESTestCase {

    @SuppressForbidden(reason = "Used specifically for the output. Only meant to be run manually, not through CI.")
    public void testCrawler() {
        Set<String> resourceAttributes = OTelSemConvCrawler.collectOTelSemConvResourceAttributes();
        System.out.println("Resource Attributes: " + resourceAttributes.size());
        for (String attribute : resourceAttributes) {
            System.out.println(attribute);
        }
    }

    @SuppressForbidden(reason = "Used specifically for the output. Only meant to be run manually, not through CI.")
    public void testEcsToOTelAttributeNames() {
        Map<String, String> attributes = EcsFieldsDiscoverer.getInstance().getEcsToOTelAttributeNames();
        System.out.println("ECS to OTel attribute mappings: " + attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            System.out.println(entry.getKey() + " --> " + entry.getValue());
        }
    }

    public void testAttributesSetUpToDate() {
        Map<String, String> ecsToOTelAttributeNames = EcsFieldsDiscoverer.getInstance().getEcsToOTelAttributeNames();
        Set<String> otelResourceAttributes = OTelSemConvCrawler.collectOTelSemConvResourceAttributes();
        Set<String> latestEcsOTelResourceAttributes = new HashSet<>();
        ecsToOTelAttributeNames.forEach((ecsAttributeName, otelAttributeName) -> {
            if (otelResourceAttributes.contains(otelAttributeName)) {
                latestEcsOTelResourceAttributes.add(ecsAttributeName);
            }
        });
        latestEcsOTelResourceAttributes.addAll(EcsFieldsDiscoverer.getInstance().getEcsResourceFields());
        boolean upToDate = latestEcsOTelResourceAttributes.equals(EcsOTelResourceAttributes.LATEST);
        if (upToDate == false) {
            printComparisonResults(latestEcsOTelResourceAttributes);
        } else {
            System.out.println("Latest ECS-to-OTel resource attributes set in EcsOTelResourceAttributes is up to date.");
        }
        assertTrue("Latest ECS-to-OTel resource attributes set in EcsOTelResourceAttributes is not up to date.", upToDate);
    }

    @SuppressForbidden(
        reason = "Output is used for updating the resource attributes set. Running nightly and only prints when not up to date."
    )
    private static void printComparisonResults(Set<String> latestEcsOTelResourceAttributes) {
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
