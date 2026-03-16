/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.entitlement.config.MainInstrumentationProvider;
import org.elasticsearch.entitlement.runtime.policy.PolicyChecker;
import org.elasticsearch.entitlement.runtime.registry.InstrumentationRegistryImpl;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.mock;

public class DynamicInstrumentationUtils {

    /**
     * This dumps the list of instrumented methods to a file specified by the first argument
     * or alternatively the system property `es.entitlements.dump`.
     */
    public static void main(String[] args) throws Exception {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();

        var path = requireNonNull(args.length > 0 ? args[0] : System.getProperty("es.entitlements.dump"), "destination for dump required");
        var descriptors = loadInstrumentedMethodDescriptors();
        Files.write(Path.of(path), () -> descriptors.stream().map(Descriptor::toLine).iterator(), StandardCharsets.UTF_8);
    }

    static List<Descriptor> loadInstrumentedMethodDescriptors() throws Exception {
        InstrumentationRegistryImpl instrumentationRegistry = new InstrumentationRegistryImpl(mock(PolicyChecker.class));
        new MainInstrumentationProvider().init(instrumentationRegistry);

        return instrumentationRegistry.getInstrumentedMethods()
            .entrySet()
            .stream()
            .map(entry -> new Descriptor(entry.getKey().className(), entry.getKey().methodName(), entry.getKey().parameterTypes()))
            .toList();
    }

    record Descriptor(String className, String methodName, List<String> parameterTypes) {

        private static final String SEPARATOR = "\t";

        CharSequence toLine() {
            return String.join(SEPARATOR, className, methodName, parameterTypes.toString());
        }

    }
}
