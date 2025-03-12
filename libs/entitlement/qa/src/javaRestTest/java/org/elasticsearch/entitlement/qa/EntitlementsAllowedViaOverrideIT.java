/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.core.Strings;
import org.junit.ClassRule;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class EntitlementsAllowedViaOverrideIT extends AbstractEntitlementsIT {

    private static void addPolicyOverrideSystemProperties(BiConsumer<String, Function<Path, String>> adder) {
        adder.accept("es.entitlements.policy.entitlement-test-plugin", tempDir -> {
            String policyOverride = Strings.format("""
                policy:
                  org.elasticsearch.entitlement.qa.test:
                    - load_native_libraries
                    - files:
                        - path: %s
                          mode: read
                """, tempDir.resolve("read_dir"));
            return new String(Base64.getEncoder().encode(policyOverride.getBytes(StandardCharsets.UTF_8)));
        });
    }

    @ClassRule
    public static EntitlementsTestRule testRule = new EntitlementsTestRule(
        true,
        null,
        EntitlementsAllowedViaOverrideIT::addPolicyOverrideSystemProperties
    );

    public EntitlementsAllowedViaOverrideIT(@Name("actionName") String actionName) {
        super(actionName, true);
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return Stream.of("runtime_load_library", "fileList").map(action -> new Object[] { action }).toList();
    }

    @Override
    protected String getTestRestCluster() {
        return testRule.cluster.getHttpAddresses();
    }
}
