/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.initialization;

import org.elasticsearch.entitlement.bridge.EntitlementChecker;
import org.elasticsearch.entitlement.instrumentation.CheckMethod;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
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

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class DynamicInstrumentationTests extends ESTestCase {

    public void testInstrumentedMethodsExist() throws Exception {
        List<Descriptor> descriptors = loadInstrumentedMethodDescriptors();
        assertThat(
            descriptors,
            everyItem(
                anyOf(
                    Descriptor.isKnown(),
                    // not visible
                    transformedMatch(Descriptor::className, startsWith("jdk/vm/ci/services/")),
                    // removed in JDK 24
                    is(
                        new Descriptor(
                            "sun/net/www/protocol/http/HttpURLConnection",
                            "openConnectionCheckRedirects",
                            List.of("java/net/URLConnection"),
                            null
                        )
                    )
                )
            )
        );
    }

    // allows dumping instrumented methods for entitlements according to `es.entitlements.dump`
    public void testToDumpInstrumentedMethods() throws Exception {
        assumeTrue("Location where to dump instrumented methods is required", System.getProperty("es.entitlements.dump") != null);

        List<Descriptor> descriptors = loadInstrumentedMethodDescriptors();
        Path path = Path.of(System.getProperty("es.entitlements.dump"));
        assert path.isAbsolute() : "absolute path required for es.entitlements.dump";
        Files.write(
            path,
            () -> descriptors.stream().filter(d -> d.methodDescriptor != null).map(Descriptor::toLine).iterator(),
            StandardCharsets.UTF_8
        );
    }

    private List<Descriptor> loadInstrumentedMethodDescriptors() throws Exception {
        Map<MethodKey, CheckMethod> methodsToInstrument = DynamicInstrumentation.getMethodsToInstrument(
            EntitlementCheckerUtils.getVersionSpecificCheckerClass(EntitlementChecker.class, Runtime.version().feature())
        );
        return methodsToInstrument.keySet().stream().map(DynamicInstrumentationTests::lookupDescriptor).toList();
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

        static Matcher<Descriptor> isKnown() {
            return new TypeSafeMatcher<>() {
                @Override
                protected boolean matchesSafely(Descriptor item) {
                    return item.methodDescriptor != null;
                }

                @Override
                public void describeTo(Description description) {
                    description.appendText("valid method descriptor");
                }

                @Override
                protected void describeMismatchSafely(Descriptor item, Description mismatchDescription) {
                    mismatchDescription.appendText("was ").appendValue(item.toString());
                }
            };
        }

    }
}
