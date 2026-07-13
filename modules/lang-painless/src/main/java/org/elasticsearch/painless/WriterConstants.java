/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.api.ValueIterator;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.StringConcatFactory;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * General pool of constants used during the writing phase of compilation.
 */
public final class WriterConstants {

    public static final int CLASS_VERSION = Opcodes.V1_8;
    public static final int ASM_VERSION = Opcodes.ASM5;
    public static final String BASE_INTERFACE_NAME = PainlessScript.class.getName();
    public static final Type BASE_INTERFACE_TYPE = Type.getType(PainlessScript.class);

    public static final String CLASS_NAME = BASE_INTERFACE_NAME + "$Script";
    public static final Type CLASS_TYPE = Type.getObjectType(CLASS_NAME.replace('.', '/'));

    public static final String CTOR_METHOD_NAME = "<init>";

    public static final Method CLINIT = getAsmMethod(void.class, "<clinit>");

    public static final Type PAINLESS_ERROR_TYPE = Type.getType(PainlessError.class);

    public static final Type OBJECT_TYPE = Type.getType(Object.class);

    public static final MethodType NEEDS_PARAMETER_METHOD_TYPE = MethodType.methodType(boolean.class);

    public static final Type ITERATOR_TYPE = Type.getType(Iterator.class);
    public static final Type VALUE_ITERATOR_TYPE = Type.getType(ValueIterator.class);
    public static final Method ITERATOR_HASNEXT = getAsmMethod(boolean.class, "hasNext");
    public static final Method ITERATOR_NEXT = getAsmMethod(Object.class, "next");
    public static final Method VALUE_ITERATOR_NEXT_BOOLEAN = getAsmMethod(boolean.class, "nextBoolean");
    public static final Method VALUE_ITERATOR_NEXT_BYTE = getAsmMethod(byte.class, "nextByte");
    public static final Method VALUE_ITERATOR_NEXT_SHORT = getAsmMethod(short.class, "nextShort");
    public static final Method VALUE_ITERATOR_NEXT_CHAR = getAsmMethod(char.class, "nextChar");
    public static final Method VALUE_ITERATOR_NEXT_INT = getAsmMethod(int.class, "nextInt");
    public static final Method VALUE_ITERATOR_NEXT_LONG = getAsmMethod(long.class, "nextLong");
    public static final Method VALUE_ITERATOR_NEXT_FLOAT = getAsmMethod(float.class, "nextFloat");
    public static final Method VALUE_ITERATOR_NEXT_DOUBLE = getAsmMethod(double.class, "nextDouble");

    public static final Type UTILITY_TYPE = Type.getType(Utility.class);
    public static final Method STRING_TO_CHAR = getAsmMethod(char.class, "StringTochar", String.class);
    public static final Method CHAR_TO_STRING = getAsmMethod(String.class, "charToString", char.class);

    /**
     * A Method instance for {@linkplain Pattern}. This isn't available from PainlessLookup because we intentionally don't add it
     * there so that the script can't create regexes without this syntax. Essentially, our static regex syntax has a monopoly on building
     * regexes because it can do it statically. This is both faster and prevents the script from doing something super slow like building a
     * regex per time it is run.
     */
    public static final Method PATTERN_COMPILE = getAsmMethod(Pattern.class, "compile", String.class, int.class);
    public static final Method PATTERN_MATCHER = getAsmMethod(Matcher.class, "matcher", Pattern.class, int.class, CharSequence.class);
    public static final Method MATCHER_MATCHES = getAsmMethod(boolean.class, "matches");
    public static final Method MATCHER_FIND = getAsmMethod(boolean.class, "find");

    public static final Method DEF_BOOTSTRAP_METHOD = getAsmMethod(
        CallSite.class,
        "$bootstrapDef",
        MethodHandles.Lookup.class,
        String.class,
        MethodType.class,
        int.class,
        int.class,
        Object[].class
    );
    static final Handle DEF_BOOTSTRAP_HANDLE = new Handle(
        Opcodes.H_INVOKESTATIC,
        CLASS_TYPE.getInternalName(),
        "$bootstrapDef",
        DEF_BOOTSTRAP_METHOD.getDescriptor(),
        false
    );

    public static final Type DEF_UTIL_TYPE = Type.getType(Def.class);

    public static final Method DEF_TO_P_BOOLEAN = getAsmMethod(boolean.class, "defToboolean", Object.class);

    public static final Method DEF_TO_P_BYTE_IMPLICIT = getAsmMethod(byte.class, "defTobyteImplicit", Object.class);
    public static final Method DEF_TO_P_SHORT_IMPLICIT = getAsmMethod(short.class, "defToshortImplicit", Object.class);
    public static final Method DEF_TO_P_CHAR_IMPLICIT = getAsmMethod(char.class, "defTocharImplicit", Object.class);
    public static final Method DEF_TO_P_INT_IMPLICIT = getAsmMethod(int.class, "defTointImplicit", Object.class);
    public static final Method DEF_TO_P_LONG_IMPLICIT = getAsmMethod(long.class, "defTolongImplicit", Object.class);
    public static final Method DEF_TO_P_FLOAT_IMPLICIT = getAsmMethod(float.class, "defTofloatImplicit", Object.class);
    public static final Method DEF_TO_P_DOUBLE_IMPLICIT = getAsmMethod(double.class, "defTodoubleImplicit", Object.class);
    public static final Method DEF_TO_P_BYTE_EXPLICIT = getAsmMethod(byte.class, "defTobyteExplicit", Object.class);
    public static final Method DEF_TO_P_SHORT_EXPLICIT = getAsmMethod(short.class, "defToshortExplicit", Object.class);
    public static final Method DEF_TO_P_CHAR_EXPLICIT = getAsmMethod(char.class, "defTocharExplicit", Object.class);
    public static final Method DEF_TO_P_INT_EXPLICIT = getAsmMethod(int.class, "defTointExplicit", Object.class);
    public static final Method DEF_TO_P_LONG_EXPLICIT = getAsmMethod(long.class, "defTolongExplicit", Object.class);
    public static final Method DEF_TO_P_FLOAT_EXPLICIT = getAsmMethod(float.class, "defTofloatExplicit", Object.class);
    public static final Method DEF_TO_P_DOUBLE_EXPLICIT = getAsmMethod(double.class, "defTodoubleExplicit", Object.class);

    public static final Method DEF_TO_B_BOOLEAN = getAsmMethod(Boolean.class, "defToBoolean", Object.class);

    public static final Method DEF_TO_B_BYTE_IMPLICIT = getAsmMethod(Byte.class, "defToByteImplicit", Object.class);
    public static final Method DEF_TO_B_SHORT_IMPLICIT = getAsmMethod(Short.class, "defToShortImplicit", Object.class);
    public static final Method DEF_TO_B_CHARACTER_IMPLICIT = getAsmMethod(Character.class, "defToCharacterImplicit", Object.class);
    public static final Method DEF_TO_B_INTEGER_IMPLICIT = getAsmMethod(Integer.class, "defToIntegerImplicit", Object.class);
    public static final Method DEF_TO_B_LONG_IMPLICIT = getAsmMethod(Long.class, "defToLongImplicit", Object.class);
    public static final Method DEF_TO_B_FLOAT_IMPLICIT = getAsmMethod(Float.class, "defToFloatImplicit", Object.class);
    public static final Method DEF_TO_B_DOUBLE_IMPLICIT = getAsmMethod(Double.class, "defToDoubleImplicit", Object.class);
    public static final Method DEF_TO_B_BYTE_EXPLICIT = getAsmMethod(Byte.class, "defToByteExplicit", Object.class);
    public static final Method DEF_TO_B_SHORT_EXPLICIT = getAsmMethod(Short.class, "defToShortExplicit", Object.class);
    public static final Method DEF_TO_B_CHARACTER_EXPLICIT = getAsmMethod(Character.class, "defToCharacterExplicit", Object.class);
    public static final Method DEF_TO_B_INTEGER_EXPLICIT = getAsmMethod(Integer.class, "defToIntegerExplicit", Object.class);
    public static final Method DEF_TO_B_LONG_EXPLICIT = getAsmMethod(Long.class, "defToLongExplicit", Object.class);
    public static final Method DEF_TO_B_FLOAT_EXPLICIT = getAsmMethod(Float.class, "defToFloatExplicit", Object.class);
    public static final Method DEF_TO_B_DOUBLE_EXPLICIT = getAsmMethod(Double.class, "defToDoubleExplicit", Object.class);

    public static final Method DEF_TO_STRING_IMPLICIT = getAsmMethod(String.class, "defToStringImplicit", Object.class);
    public static final Method DEF_TO_STRING_EXPLICIT = getAsmMethod(String.class, "defToStringExplicit", Object.class);

    /** invokedynamic bootstrap for lambda expression/method references */
    public static final MethodType LAMBDA_BOOTSTRAP_TYPE = MethodType.methodType(
        CallSite.class,
        MethodHandles.Lookup.class,
        String.class,
        MethodType.class,
        MethodType.class,
        String.class,
        int.class,
        String.class,
        MethodType.class,
        int.class,
        int.class,
        Object[].class
    );
    public static final Handle LAMBDA_BOOTSTRAP_HANDLE = new Handle(
        Opcodes.H_INVOKESTATIC,
        Type.getInternalName(LambdaBootstrap.class),
        "lambdaBootstrap",
        LAMBDA_BOOTSTRAP_TYPE.toMethodDescriptorString(),
        false
    );
    public static final MethodType DELEGATE_BOOTSTRAP_TYPE = MethodType.methodType(
        CallSite.class,
        MethodHandles.Lookup.class,
        String.class,
        MethodType.class,
        MethodHandle.class,
        int.class,
        Object[].class
    );
    public static final Handle DELEGATE_BOOTSTRAP_HANDLE = new Handle(
        Opcodes.H_INVOKESTATIC,
        Type.getInternalName(LambdaBootstrap.class),
        "delegateBootstrap",
        DELEGATE_BOOTSTRAP_TYPE.toMethodDescriptorString(),
        false
    );

    public static final MethodType MAKE_CONCAT_TYPE = MethodType.methodType(
        CallSite.class,
        MethodHandles.Lookup.class,
        String.class,
        MethodType.class
    );
    public static final Handle STRING_CONCAT_BOOTSTRAP_HANDLE = new Handle(
        Opcodes.H_INVOKESTATIC,
        Type.getInternalName(StringConcatFactory.class),
        "makeConcat",
        MAKE_CONCAT_TYPE.toMethodDescriptorString(),
        false
    );

    public static final int MAX_STRING_CONCAT_ARGS = 200;

    public static final Type STRING_TYPE = Type.getType(String.class);

    public static final Type OBJECTS_TYPE = Type.getType(Objects.class);
    public static final Method EQUALS = getAsmMethod(boolean.class, "equals", Object.class, Object.class);

    public static final Type COLLECTION_TYPE = Type.getType(Collection.class);
    public static final Method COLLECTION_SIZE = getAsmMethod(int.class, "size");

    public static final Type RUNNABLE_TYPE = Type.getType(Runnable.class);
    public static final Method RUNNABLE_RUN = getAsmMethod(void.class, "run");

    /**
     * Decrement interval for the persistent cancellation poll counter.  The counter is stored in
     * {@link #CANCEL_POLL_FIELD} on the generated script instance and is decremented at every
     * function entry and loop back-edge.  The cancellation runnable fires (and the counter resets)
     * once every {@code CANCELLATION_POLL_INTERVAL} decrements, amortising the check cost.
     */
    public static final int CANCELLATION_POLL_INTERVAL = 1000;

    /** Name of the synthetic {@code int} field added to opted-in generated script classes. */
    public static final String CANCEL_POLL_FIELD = "$cancelPoll";

    public static final Method GET_CANCELLATION_CHECK = getAsmMethod(Runnable.class, "_getCancellationCheck");

    /**
     * Method generated on opted-in script classes that performs one decrement-and-check against
     * {@link #CANCEL_POLL_FIELD}, letting {@code @script_aware} augmentations poll the script's
     * persistent counter directly instead of maintaining their own — so the count is shared with the
     * compiler's inline loop/entry decrements and stays amortised across all script work.
     */
    public static final Method POLL_CANCELLATION = getAsmMethod(void.class, "_pollCancellation");

    /**
     * Name of the synthetic {@code long} field added to generated script classes when allocation tracking is enabled
     * (a positive per-context limit). Holds the running heuristic allocation total, reset at every {@code execute} entry.
     */
    public static final String ALLOC_BYTES_FIELD = "$allocBytes";

    /** Generated override of {@link org.elasticsearch.painless.PainlessScript#$incAllocBytes(long)}. */
    public static final Method INC_ALLOC_BYTES = getAsmMethod(long.class, "$incAllocBytes", long.class);

    /** Generated override of {@link org.elasticsearch.painless.PainlessScript#getAllocBytes()}. */
    public static final Method GET_ALLOC_BYTES = getAsmMethod(long.class, "getAllocBytes");

    /**
     * {@link org.elasticsearch.painless.PainlessScript#$checkAllocBytes(long)} — invoked (via the script interface) at every
     * compile-time-known allocation site to charge and check the running total against the per-context limit.
     */
    public static final Method CHECK_ALLOC_BYTES = getAsmMethod(void.class, "$checkAllocBytes", long.class);

    /** ASM {@link Type} for {@link AllocationGuard}, the log-and-throw helper called on a limit breach. */
    public static final Type ALLOCATION_GUARD_TYPE = Type.getType(AllocationGuard.class);

    /** {@link AllocationGuard#allocationLimitExceeded(long, long, long)} — called from {@code $checkAllocBytes} on breach. */
    public static final Method ALLOCATION_LIMIT_EXCEEDED = getAsmMethod(
        void.class,
        "allocationLimitExceeded",
        long.class,
        long.class,
        long.class
    );

    /** {@link AllocationGuard#sanitizeEstimate(long)} — normalizes an {@code @allocates_dynamic} estimator's result. */
    public static final Method SANITIZE_ESTIMATE = getAsmMethod(long.class, "sanitizeEstimate", long.class);

    /** ASM {@link Type} for {@link AllocSizes}, the sizing helpers invoked from generated bytecode for runtime-sized arrays. */
    public static final Type ALLOC_SIZES_TYPE = Type.getType(AllocSizes.class);

    /** {@link AllocSizes#mulSat(long, long)} — saturating multiply folding multi-dim array extents into one element count. */
    public static final Method ALLOC_MUL_SAT = getAsmMethod(long.class, "mulSat", long.class, long.class);

    /** {@link AllocSizes#arrayBytes(long, int)} — saturating {@code pad8(ARRAY_HEADER + fieldSize * length)} for an array. */
    public static final Method ALLOC_ARRAY_BYTES = getAsmMethod(long.class, "arrayBytes", long.class, int.class);

    /** {@link AllocSizes#stringConcatOperandBytes(Object)} — runtime byte cost of a reference operand in a string concat. */
    public static final Method ALLOC_STRING_CONCAT_OPERAND_BYTES = getAsmMethod(long.class, "stringConcatOperandBytes", Object.class);

    private static Method getAsmMethod(final Class<?> rtype, final String name, final Class<?>... ptypes) {
        return new Method(name, MethodType.methodType(rtype, ptypes).toMethodDescriptorString());
    }

    private WriterConstants() {}
}
