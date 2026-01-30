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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DynamicInstrumentationUtils {

    /**
     * This dumps the list of instrumented methods to a file specified by the first argument
     * or alternatively the system property `es.entitlements.dump`.
     */
    public static void main(String[] args) throws Exception {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();

        var path = requireNonNull(args.length > 0 ? args[0] : System.getProperty("es.entitlements.dump"), "destination for dump required");
        // TODO - Update this to use EntitlementRegistry
        // var descriptors = loadInstrumentedMethodDescriptors();
        // Files.write(
        // Path.of(path),
        // () -> descriptors.stream().filter(d -> d.methodDescriptor != null).map(Descriptor::toLine).iterator(),
        // StandardCharsets.UTF_8
        // );
    }

    record Descriptor(String className, String methodName, List<String> parameterTypes, String methodDescriptor) {

        private static final String SEPARATOR = "\t";

        CharSequence toLine() {
            return String.join(SEPARATOR, className, methodName, methodDescriptor);
        }

    }
}
