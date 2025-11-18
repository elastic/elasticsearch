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
import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.instrumentation.CheckMethod;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
        var descriptors = loadInstrumentedMethodDescriptors();
        Files.write(
            Path.of(path),
            () -> descriptors.stream().filter(d -> d.methodDescriptor != null).map(Descriptor::toLine).iterator(),
            StandardCharsets.UTF_8
        );
    }

    static List<Descriptor> loadInstrumentedMethodDescriptors() throws Exception {
        Map<MethodKey, CheckMethod> methodsToInstrument = DynamicInstrumentation.getMethodsToInstrument(
            EntitlementCheckerUtils.getVersionSpecificCheckerClass(EntitlementChecker.class, Runtime.version().feature())
        );
        return methodsToInstrument.keySet().stream().map(DynamicInstrumentationUtils::lookupDescriptor).toList();
    }

    private static Descriptor lookupDescriptor(MethodKey key) {
        final String[] foundDescriptor = { null };
        try {
            ClassReader reader = new ClassReader(key.className());
            reader.accept(new ClassVisitor(Opcodes.ASM9) {
                @Override
                public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
                    if (name.equals(key.methodName()) == false) {
                        return null;
                    }
                    List<String> argTypes = Stream.of(Type.getArgumentTypes(descriptor)).map(Type::getInternalName).toList();
                    if (argTypes.equals(key.parameterTypes())) {
                        foundDescriptor[0] = descriptor;
                    }
                    return null;
                }
            }, 0);
        } catch (IOException e) {
            // nothing to do
        }
        return new Descriptor(key.className(), key.methodName(), key.parameterTypes(), foundDescriptor[0]);

    }

    record Descriptor(String className, String methodName, List<String> parameterTypes, String methodDescriptor) {

        private static final String SEPARATOR = "\t";

        CharSequence toLine() {
            return String.join(SEPARATOR, className, methodName, methodDescriptor);
        }

    }
}
