/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.api.Augmentation;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.elasticsearch.script.ScriptException;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.time.ZonedDateTime;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
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
    public static final Method CONVERT_TO_SCRIPT_EXCEPTION_METHOD = getAsmMethod(ScriptException.class, "convertToScriptException",
            Throwable.class, Map.class);

    public static final String CLASS_NAME = BASE_INTERFACE_NAME + "$Script";
    public static final Type CLASS_TYPE   = Type.getObjectType(CLASS_NAME.replace('.', '/'));

    public static final String CTOR_METHOD_NAME = "<init>";

    public static final Method CLINIT      = getAsmMethod(void.class, "<clinit>");

    public static final String GET_NAME_NAME = "getName";
    public static final Method GET_NAME_METHOD = getAsmMethod(String.class, GET_NAME_NAME);

    public static final String GET_SOURCE_NAME = "getSource";
    public static final Method GET_SOURCE_METHOD = getAsmMethod(String.class, GET_SOURCE_NAME);

    public static final String GET_STATEMENTS_NAME = "getStatements";
    public static final Method GET_STATEMENTS_METHOD = getAsmMethod(BitSet.class, GET_STATEMENTS_NAME);

    // All of these types are caught by the main method and rethrown as ScriptException
    public static final Type PAINLESS_ERROR_TYPE         = Type.getType(PainlessError.class);
    public static final Type BOOTSTRAP_METHOD_ERROR_TYPE = Type.getType(BootstrapMethodError.class);
    public static final Type OUT_OF_MEMORY_ERROR_TYPE    = Type.getType(OutOfMemoryError.class);
    public static final Type STACK_OVERFLOW_ERROR_TYPE   = Type.getType(StackOverflowError.class);
    public static final Type EXCEPTION_TYPE              = Type.getType(Exception.class);
    public static final Type PAINLESS_EXPLAIN_ERROR_TYPE = Type.getType(PainlessExplainError.class);
    public static final Method PAINLESS_EXPLAIN_ERROR_GET_HEADERS_METHOD = getAsmMethod(Map.class, "getHeaders", PainlessLookup.class);

    public static final Type OBJECT_TYPE = Type.getType(Object.class);
    public static final Type BITSET_TYPE = Type.getType(BitSet.class);

    public static final Type DEFINITION_TYPE = Type.getType(PainlessLookup.class);

    public static final Type COLLECTIONS_TYPE = Type.getType(Collections.class);
    public static final Method EMPTY_MAP_METHOD = getAsmMethod(Map.class, "emptyMap");

    public static final MethodType NEEDS_PARAMETER_METHOD_TYPE = MethodType.methodType(boolean.class);

    public static final Type MAP_TYPE  = Type.getType(Map.class);
    public static final Method MAP_GET = getAsmMethod(Object.class, "get", Object.class);

    public static final Type ITERATOR_TYPE = Type.getType(Iterator.class);
    public static final Method ITERATOR_HASNEXT = getAsmMethod(boolean.class, "hasNext");
    public static final Method ITERATOR_NEXT = getAsmMethod(Object.class, "next");

    public static final Type UTILITY_TYPE = Type.getType(Utility.class);
    public static final Method STRING_TO_CHAR = getAsmMethod(char.class, "StringTochar", String.class);
    public static final Method CHAR_TO_STRING = getAsmMethod(String.class, "charToString", char.class);

    public static final Type FUNCTION_TABLE_TYPE = Type.getType(FunctionTable.class);

    // TODO: remove this when the transition from Joda to Java datetimes is completed
    public static final Method JCZDT_TO_ZONEDDATETIME =
            getAsmMethod(ZonedDateTime.class, "JCZDTToZonedDateTime", JodaCompatibleZonedDateTime.class);

    public static final Type METHOD_HANDLE_TYPE = Type.getType(MethodHandle.class);

    public static final Type AUGMENTATION_TYPE = Type.getType(Augmentation.class);

    /**
     * A Method instance for {@linkplain Pattern}. This isn't available from PainlessLookup because we intentionally don't add it
     * there so that the script can't create regexes without this syntax. Essentially, our static regex syntax has a monopoly on building
     * regexes because it can do it statically. This is both faster and prevents the script from doing something super slow like building a
     * regex per time it is run.
     */
    public static final Method PATTERN_COMPILE = getAsmMethod(Pattern.class, "compile", String.class, int.class);
    public static final Method PATTERN_MATCHER = getAsmMethod(Matcher.class, "matcher", CharSequence.class);
    public static final Method MATCHER_MATCHES = getAsmMethod(boolean.class, "matches");
    public static final Method MATCHER_FIND = getAsmMethod(boolean.class, "find");

    public static final Method DEF_BOOTSTRAP_METHOD = getAsmMethod(CallSite.class, "$bootstrapDef", MethodHandles.Lookup.class,
            String.class, MethodType.class, int.class, int.class, Object[].class);
    static final Handle DEF_BOOTSTRAP_HANDLE = new Handle(Opcodes.H_INVOKESTATIC, CLASS_TYPE.getInternalName(), "$bootstrapDef",
            DEF_BOOTSTRAP_METHOD.getDescriptor(), false);
    public static final Type DEF_BOOTSTRAP_DELEGATE_TYPE = Type.getType(DefBootstrap.class);
    public static final Method DEF_BOOTSTRAP_DELEGATE_METHOD = getAsmMethod(CallSite.class, "bootstrap", PainlessLookup.class,
            FunctionTable.class, MethodHandles.Lookup.class, String.class, MethodType.class, int.class, int.class, Object[].class);

    public static final Type DEF_UTIL_TYPE = Type.getType(Def.class);

    public static final Method DEF_TO_P_BOOLEAN = getAsmMethod(boolean.class, "defToboolean", Object.class);

    public static final Method DEF_TO_P_BYTE_IMPLICIT   = getAsmMethod(byte.class   , "defTobyteImplicit"   , Object.class);
    public static final Method DEF_TO_P_SHORT_IMPLICIT  = getAsmMethod(short.class  , "defToshortImplicit"  , Object.class);
    public static final Method DEF_TO_P_CHAR_IMPLICIT   = getAsmMethod(char.class   , "defTocharImplicit"   , Object.class);
    public static final Method DEF_TO_P_INT_IMPLICIT    = getAsmMethod(int.class    , "defTointImplicit"    , Object.class);
    public static final Method DEF_TO_P_LONG_IMPLICIT   = getAsmMethod(long.class   , "defTolongImplicit"   , Object.class);
    public static final Method DEF_TO_P_FLOAT_IMPLICIT  = getAsmMethod(float.class  , "defTofloatImplicit"  , Object.class);
    public static final Method DEF_TO_P_DOUBLE_IMPLICIT = getAsmMethod(double.class , "defTodoubleImplicit" , Object.class);
    public static final Method DEF_TO_P_BYTE_EXPLICIT   = getAsmMethod(byte.class   , "defTobyteExplicit"   , Object.class);
    public static final Method DEF_TO_P_SHORT_EXPLICIT  = getAsmMethod(short.class  , "defToshortExplicit"  , Object.class);
    public static final Method DEF_TO_P_CHAR_EXPLICIT   = getAsmMethod(char.class   , "defTocharExplicit"   , Object.class);
    public static final Method DEF_TO_P_INT_EXPLICIT    = getAsmMethod(int.class    , "defTointExplicit"    , Object.class);
    public static final Method DEF_TO_P_LONG_EXPLICIT   = getAsmMethod(long.class   , "defTolongExplicit"   , Object.class);
    public static final Method DEF_TO_P_FLOAT_EXPLICIT  = getAsmMethod(float.class  , "defTofloatExplicit"  , Object.class);
    public static final Method DEF_TO_P_DOUBLE_EXPLICIT = getAsmMethod(double.class , "defTodoubleExplicit" , Object.class);

    public static final Method DEF_TO_B_BOOLEAN = getAsmMethod(Boolean.class, "defToBoolean", Object.class);

    public static final Method DEF_TO_B_BYTE_IMPLICIT      = getAsmMethod(Byte.class      , "defToByteImplicit"      , Object.class);
    public static final Method DEF_TO_B_SHORT_IMPLICIT     = getAsmMethod(Short.class     , "defToShortImplicit"     , Object.class);
    public static final Method DEF_TO_B_CHARACTER_IMPLICIT = getAsmMethod(Character.class , "defToCharacterImplicit" , Object.class);
    public static final Method DEF_TO_B_INTEGER_IMPLICIT   = getAsmMethod(Integer.class   , "defToIntegerImplicit"   , Object.class);
    public static final Method DEF_TO_B_LONG_IMPLICIT      = getAsmMethod(Long.class      , "defToLongImplicit"      , Object.class);
    public static final Method DEF_TO_B_FLOAT_IMPLICIT     = getAsmMethod(Float.class     , "defToFloatImplicit"     , Object.class);
    public static final Method DEF_TO_B_DOUBLE_IMPLICIT    = getAsmMethod(Double.class    , "defToDoubleImplicit"    , Object.class);
    public static final Method DEF_TO_B_BYTE_EXPLICIT      = getAsmMethod(Byte.class      , "defToByteExplicit"      , Object.class);
    public static final Method DEF_TO_B_SHORT_EXPLICIT     = getAsmMethod(Short.class     , "defToShortExplicit"     , Object.class);
    public static final Method DEF_TO_B_CHARACTER_EXPLICIT = getAsmMethod(Character.class , "defToCharacterExplicit" , Object.class);
    public static final Method DEF_TO_B_INTEGER_EXPLICIT   = getAsmMethod(Integer.class   , "defToIntegerExplicit"   , Object.class);
    public static final Method DEF_TO_B_LONG_EXPLICIT      = getAsmMethod(Long.class      , "defToLongExplicit"      , Object.class);
    public static final Method DEF_TO_B_FLOAT_EXPLICIT     = getAsmMethod(Float.class     , "defToFloatExplicit"     , Object.class);
    public static final Method DEF_TO_B_DOUBLE_EXPLICIT    = getAsmMethod(Double.class    , "defToDoubleExplicit"    , Object.class);

    public static final Method DEF_TO_STRING_IMPLICIT = getAsmMethod(String.class, "defToStringImplicit", Object.class);
    public static final Method DEF_TO_STRING_EXPLICIT = getAsmMethod(String.class, "defToStringExplicit", Object.class);

    // TODO: remove this when the transition from Joda to Java datetimes is completed
    public static final Method DEF_TO_ZONEDDATETIME = getAsmMethod(ZonedDateTime.class, "defToZonedDateTime", Object.class);

    public static final Type DEF_ARRAY_LENGTH_METHOD_TYPE = Type.getMethodType(Type.INT_TYPE, Type.getType(Object.class));

    /** invokedynamic bootstrap for lambda expression/method references */
    public static final MethodType LAMBDA_BOOTSTRAP_TYPE =
            MethodType.methodType(CallSite.class, MethodHandles.Lookup.class, String.class, MethodType.class,
                    MethodType.class, String.class, int.class, String.class, MethodType.class, int.class);
    public static final Handle LAMBDA_BOOTSTRAP_HANDLE =
            new Handle(Opcodes.H_INVOKESTATIC, Type.getInternalName(LambdaBootstrap.class),
                "lambdaBootstrap", LAMBDA_BOOTSTRAP_TYPE.toMethodDescriptorString(), false);
    public static final MethodType DELEGATE_BOOTSTRAP_TYPE =
        MethodType.methodType(CallSite.class, MethodHandles.Lookup.class, String.class, MethodType.class, MethodHandle.class);
    public static final Handle DELEGATE_BOOTSTRAP_HANDLE =
        new Handle(Opcodes.H_INVOKESTATIC, Type.getInternalName(LambdaBootstrap.class),
            "delegateBootstrap", DELEGATE_BOOTSTRAP_TYPE.toMethodDescriptorString(), false);

    /** dynamic invokedynamic bootstrap for indy string concats (Java 9+) */
    public static final Handle INDY_STRING_CONCAT_BOOTSTRAP_HANDLE;
    static {
        Handle bs;
        try {
            final Class<?> factory = Class.forName("java.lang.invoke.StringConcatFactory");
            final String methodName = "makeConcat";
            final MethodType type = MethodType.methodType(CallSite.class, MethodHandles.Lookup.class, String.class, MethodType.class);
            // ensure it is there:
            MethodHandles.publicLookup().findStatic(factory, methodName, type);
            bs = new Handle(Opcodes.H_INVOKESTATIC, Type.getInternalName(factory), methodName, type.toMethodDescriptorString(), false);
        } catch (ReflectiveOperationException e) {
            // not Java 9 - we set it null, so MethodWriter uses StringBuilder:
            bs = null;
        }
        INDY_STRING_CONCAT_BOOTSTRAP_HANDLE = bs;
    }

    public static final int MAX_INDY_STRING_CONCAT_ARGS = 200;

    public static final Type STRING_TYPE = Type.getType(String.class);
    public static final Type STRINGBUILDER_TYPE = Type.getType(StringBuilder.class);

    public static final Method STRINGBUILDER_CONSTRUCTOR    = getAsmMethod(void.class, CTOR_METHOD_NAME);
    public static final Method STRINGBUILDER_APPEND_BOOLEAN = getAsmMethod(StringBuilder.class, "append", boolean.class);
    public static final Method STRINGBUILDER_APPEND_CHAR    = getAsmMethod(StringBuilder.class, "append", char.class);
    public static final Method STRINGBUILDER_APPEND_INT     = getAsmMethod(StringBuilder.class, "append", int.class);
    public static final Method STRINGBUILDER_APPEND_LONG    = getAsmMethod(StringBuilder.class, "append", long.class);
    public static final Method STRINGBUILDER_APPEND_FLOAT   = getAsmMethod(StringBuilder.class, "append", float.class);
    public static final Method STRINGBUILDER_APPEND_DOUBLE  = getAsmMethod(StringBuilder.class, "append", double.class);
    public static final Method STRINGBUILDER_APPEND_STRING  = getAsmMethod(StringBuilder.class, "append", String.class);
    public static final Method STRINGBUILDER_APPEND_OBJECT  = getAsmMethod(StringBuilder.class, "append", Object.class);
    public static final Method STRINGBUILDER_TOSTRING       = getAsmMethod(String.class, "toString");

    public static final Type OBJECTS_TYPE = Type.getType(Objects.class);
    public static final Method EQUALS = getAsmMethod(boolean.class, "equals", Object.class, Object.class);

    public static final Type COLLECTION_TYPE = Type.getType(Collection.class);
    public static final Method COLLECTION_SIZE = getAsmMethod(int.class, "size");

    private static Method getAsmMethod(final Class<?> rtype, final String name, final Class<?>... ptypes) {
        return new Method(name, MethodType.methodType(rtype, ptypes).toMethodDescriptorString());
    }

    private WriterConstants() {}
}
