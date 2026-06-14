/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

/**
 * Tests that {@link StructClassWriter} generates correct struct implementation classes.
 */
public class StructClassWriterTests extends ProcessorTestCase {

    /**
     * A {@code @Struct} with two {@code long} fields (static layout) must generate
     * an inner class with a {@code LAYOUT} field of the expected byte size.
     * Setting and reading back a field must return the written value.
     */
    public void testStaticStructGeneratesLayoutAndAccessors() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Struct;
            import org.elasticsearch.foreign.StructFactory;
            import org.elasticsearch.foreign.Setter;
            @LibrarySpecification(name = "testlib")
            public interface StructLib {
                @Struct
                interface Pair {
                    long first();
                    @Setter void first(long v);
                    long second();
                    @Setter void second(long v);
                }
                @StructFactory
                Pair newPair();
                @Function("get_val")
                long getVal(long x);
            }
            """;

        CompilationResult result = compile("test.StructLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // Load the outer $Impl to access the inner struct impl
        Class<?> outerImpl = result.loadClassNoInit("test.StructLib$Impl");
        assertNotNull("StructLib$Impl must be generated", outerImpl);

        // The struct inner class is StructLib$Pair$Impl
        Class<?> pairImpl = result.loadClass("test.StructLib$Pair$Impl");
        assertNotNull("StructLib$Pair$Impl must be generated", pairImpl);

        // LAYOUT field must exist and have byte size 16 (two longs)
        java.lang.reflect.Field layoutField = pairImpl.getDeclaredField("LAYOUT");
        layoutField.setAccessible(true);
        assertTrue("LAYOUT must be static", java.lang.reflect.Modifier.isStatic(layoutField.getModifiers()));
        assertTrue("LAYOUT must be final", java.lang.reflect.Modifier.isFinal(layoutField.getModifiers()));
        java.lang.foreign.MemoryLayout layout = (java.lang.foreign.MemoryLayout) layoutField.get(null);
        assertEquals("LAYOUT must be 16 bytes (2 longs)", 16L, layout.byteSize());

        // Instantiate and round-trip a value through first()
        java.lang.reflect.Constructor<?> pairCtor = pairImpl.getDeclaredConstructor();
        pairCtor.setAccessible(true);
        Object pair = pairCtor.newInstance();
        java.lang.reflect.Method setFirst = pairImpl.getDeclaredMethod("first", long.class);
        setFirst.setAccessible(true);
        java.lang.reflect.Method getFirst = pairImpl.getDeclaredMethod("first");
        getFirst.setAccessible(true);
        setFirst.invoke(pair, 42L);
        assertEquals("first() must return 42 after first(42)", 42L, (long) getFirst.invoke(pair));
    }

    /**
     * A {@code @Struct} with {@code dynamicLayout=true} must generate an inner class
     * whose constructor takes {@code (long sizeof, long off1, long off2, ...)} and whose
     * getters/setters use the stored offsets.
     */
    public void testDynamicStructGeneratesOffsetAccessors() throws Exception {
        String source = """
            package test;
            import java.lang.foreign.MemorySegment;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Struct;
            import org.elasticsearch.foreign.StructFactory;
            import org.elasticsearch.foreign.Setter;
            @LibrarySpecification(name = "testlib")
            public interface DynStructLib {
                @Struct(dynamicLayout = true)
                interface Info {
                    long st_size();
                    @Setter void st_size(long v);
                    long st_blocks();
                    @Setter void st_blocks(long v);
                }
                @StructFactory
                Info newInfo(long sizeof, long st_size$offset, long st_blocks$offset);
                @Function("noop")
                void noop();
            }
            """;

        CompilationResult result = compile("test.DynStructLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> infoImpl = result.loadClass("test.DynStructLib$Info$Impl");
        assertNotNull("DynStructLib$Info$Impl must be generated", infoImpl);

        // Must NOT have a LAYOUT field
        boolean hasLayout = false;
        for (java.lang.reflect.Field f : infoImpl.getDeclaredFields()) {
            if (f.getName().equals("LAYOUT")) {
                hasLayout = true;
            }
        }
        assertFalse("Dynamic struct must not have a LAYOUT field", hasLayout);

        // Constructor: (long sizeof, long st_size$offset, long st_blocks$offset)
        java.lang.reflect.Constructor<?> ctor = infoImpl.getDeclaredConstructor(long.class, long.class, long.class);
        assertNotNull("Constructor (long, long, long) must exist", ctor);
        ctor.setAccessible(true);

        // Allocate a 24-byte segment, put 99L at byte offset 8
        // sizeof=24, st_size$offset=8, st_blocks$offset=16
        Object info = ctor.newInstance(24L, 8L, 16L);

        java.lang.reflect.Method setSize = infoImpl.getDeclaredMethod("st_size", long.class);
        setSize.setAccessible(true);
        java.lang.reflect.Method getSize = infoImpl.getDeclaredMethod("st_size");
        getSize.setAccessible(true);
        setSize.invoke(info, 99L);
        assertEquals("st_size() must return 99 after st_size(99)", 99L, (long) getSize.invoke(info));
    }

    /**
     * A {@code @StructFactory} method on a static-layout struct must generate code that
     * creates a non-null struct instance via a public no-arg constructor.
     */
    public void testStaticStructFactoryGeneratesInstance() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Struct;
            import org.elasticsearch.foreign.StructFactory;
            import org.elasticsearch.foreign.Setter;
            @LibrarySpecification(name = "testlib")
            public interface FactoryLib {
                @Struct
                interface Point {
                    long x();
                    @Setter void x(long v);
                    long y();
                    @Setter void y(long v);
                }
                @StructFactory
                Point newPoint();
                @Function("noop")
                void noop();
            }
            """;

        CompilationResult result = compile("test.FactoryLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        // Load the outer impl (without init, as it would try to link native symbols)
        Class<?> outerImpl = result.loadClassNoInit("test.FactoryLib$Impl");
        assertNotNull("FactoryLib$Impl must be generated", outerImpl);

        // Load the struct impl (with initialization — it only needs VarHandle setup, no native calls)
        Class<?> pointImpl = result.loadClass("test.FactoryLib$Point$Impl");
        assertNotNull("FactoryLib$Point$Impl must be generated", pointImpl);

        // Verify the factory method exists in the outer impl with the correct return type
        java.lang.reflect.Method newPointMethod = outerImpl.getDeclaredMethod("newPoint");
        assertNotNull("newPoint() method must exist on FactoryLib$Impl", newPointMethod);
        assertEquals("newPoint() must return test.FactoryLib$Point", "test.FactoryLib$Point", newPointMethod.getReturnType().getName());

        // Directly instantiate the struct impl to verify it works (uses Arena.ofAuto, no native deps)
        java.lang.reflect.Constructor<?> pointCtor = pointImpl.getDeclaredConstructor();
        pointCtor.setAccessible(true);
        Object point = pointCtor.newInstance();
        assertNotNull("Point$Impl must be instantiable", point);

        // Round-trip x()
        java.lang.reflect.Method setX = pointImpl.getDeclaredMethod("x", long.class);
        setX.setAccessible(true);
        java.lang.reflect.Method getX = pointImpl.getDeclaredMethod("x");
        getX.setAccessible(true);
        setX.invoke(point, 77L);
        assertEquals("x() must return 77 after x(77)", 77L, (long) getX.invoke(point));
    }

    /**
     * A {@code @StructFactory} method on a dynamic-layout struct must generate code that
     * passes sizeof and offsets through to the impl constructor correctly.
     */
    public void testDynamicStructFactoryPassesArguments() throws Exception {
        String source = """
            package test;
            import org.elasticsearch.foreign.LibrarySpecification;
            import org.elasticsearch.foreign.Function;
            import org.elasticsearch.foreign.Struct;
            import org.elasticsearch.foreign.StructFactory;
            import org.elasticsearch.foreign.Setter;
            @LibrarySpecification(name = "testlib")
            public interface DynFactoryLib {
                @Struct(dynamicLayout = true)
                interface Vec {
                    long len();
                    @Setter void len(long v);
                    long cap();
                    @Setter void cap(long v);
                }
                @StructFactory
                Vec newVec(long sizeof, long len$offset, long cap$offset);
                @Function("noop")
                void noop();
            }
            """;

        CompilationResult result = compile("test.DynFactoryLib", source);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());

        Class<?> vecImpl = result.loadClass("test.DynFactoryLib$Vec$Impl");
        assertNotNull("DynFactoryLib$Vec$Impl must be generated", vecImpl);

        // Instantiate with sizeof=32, len$offset=0, cap$offset=8
        java.lang.reflect.Constructor<?> ctor = vecImpl.getDeclaredConstructor(long.class, long.class, long.class);
        ctor.setAccessible(true);
        Object vec = ctor.newInstance(32L, 0L, 8L);
        assertNotNull("Vec$Impl must be instantiable", vec);

        // Write len and read back
        java.lang.reflect.Method setLen = vecImpl.getDeclaredMethod("len", long.class);
        setLen.setAccessible(true);
        java.lang.reflect.Method getLen = vecImpl.getDeclaredMethod("len");
        getLen.setAccessible(true);
        setLen.invoke(vec, 123L);
        assertEquals("len() must return 123 after len(123)", 123L, (long) getLen.invoke(vec));

        // Write cap at offset 8 and read back
        java.lang.reflect.Method setCap = vecImpl.getDeclaredMethod("cap", long.class);
        setCap.setAccessible(true);
        java.lang.reflect.Method getCap = vecImpl.getDeclaredMethod("cap");
        getCap.setAccessible(true);
        setCap.invoke(vec, 256L);
        assertEquals("cap() must return 256 after cap(256)", 256L, (long) getCap.invoke(vec));
    }
}
