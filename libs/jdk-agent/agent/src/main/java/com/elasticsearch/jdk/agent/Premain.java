/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package com.elasticsearch.jdk.agent;

import com.elasticsearch.jdk.agent.instrument.math.FastDoubleClassWriter;

import java.io.PrintStream;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.security.ProtectionDomain;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class Premain {

    private static final PrintStream traceOutput = System.out;
    private static final PrintStream errorOutput = System.err;

    /**
     * Map of (binary) class name to respective transformer function.
     */
    private static final Map<String, Function<byte[], byte[]>> TRANSFORMS = Map.of(
        "java.lang.Double",
        b -> (new FastDoubleClassWriter(b, traceOutput)).get()
    );

    public static void premain(String agentArgs, Instrumentation inst) {
        inst.addTransformer(new ClassFileTransformer() {
            @Override
            public byte[] transform(ClassLoader l, String name, Class<?> c, ProtectionDomain d, byte[] b) {
                var transformer = TRANSFORMS.get(toBinaryName(name));
                if (transformer != null) {
                    trace("Transforming %s", toBinaryName(name));
                    try {
                        return transformer.apply(b);
                    } catch (Throwable t) {
                        error("Failed to transform %s with error: %s", toBinaryName(name), t.getMessage());
                    }
                }
                return b; // the original class bytes
            }
        }, true);

        Class<?>[] classes = inst.getAllLoadedClasses();
        List<Class<?>> loadedToPatch = Arrays.stream(classes)
            .filter(c -> c != null && c.getName() != null && TRANSFORMS.containsKey(c.getName()))
            .toList();

        // TODO: assert expected retransform classes found

        for (Class<?> c : loadedToPatch) {
            try {
                inst.retransformClasses(c);
            } catch (UnmodifiableClassException x) {
                error("Cannot retransform class: %s, error: %s", c.getName(), x.getMessage());
            }
        }
    }

    /** Returns the binary form of the given internal class name. */
    static String toBinaryName(String clsName) {
        return clsName.replace('/', '.');
    }

    private static void trace(String fmt, Object... args) {
        if (traceOutput != null) {
            traceOutput.format(fmt, args);
            traceOutput.println();
        }
    }

    private static void error(String fmt, Object... args) {
        if (errorOutput != null) {
            errorOutput.format(fmt, args);
            errorOutput.println();
        }
    }
}
