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

import org.apache.lucene.search.Scorer;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;

/**
 * General pool of constants used during the writing phase of compilation.
 */
public final class WriterConstants {

    public final static String BASE_CLASS_NAME = Executable.class.getName();
    public final static Type BASE_CLASS_TYPE   = Type.getType(Executable.class);

    public final static String CLASS_NAME      = BASE_CLASS_NAME + "$Script";
    public final static Type CLASS_TYPE        = Type.getObjectType(CLASS_NAME.replace('.', '/'));

    public final static Method CONSTRUCTOR = getAsmMethod(void.class, "<init>", String.class, String.class, BitSet.class);
    public final static Method EXECUTE     =
        getAsmMethod(Object.class, "execute", Map.class, Scorer.class, LeafDocLookup.class, Object.class);

    public final static Type PAINLESS_ERROR_TYPE = Type.getType(PainlessError.class);

    public final static Type NEEDS_SCORE_TYPE = Type.getType(NeedsScore.class);
    public final static Type SCORER_TYPE = Type.getType(Scorer.class);
    public final static Method SCORER_SCORE = getAsmMethod(float.class, "score");

    public final static Type MAP_TYPE  = Type.getType(Map.class);
    public final static Method MAP_GET = getAsmMethod(Object.class, "get", Object.class);

    public final static Type ITERATOR_TYPE = Type.getType(Iterator.class);
    public final static Method ITERATOR_HASNEXT = getAsmMethod(boolean.class, "hasNext");
    public final static Method ITERATOR_NEXT = getAsmMethod(Object.class, "next");

    public final static Type UTILITY_TYPE = Type.getType(Utility.class);
    public final static Method STRING_TO_CHAR = getAsmMethod(char.class, "StringTochar", String.class);
    public final static Method CHAR_TO_STRING = getAsmMethod(String.class, "charToString", char.class);

    /** dynamic callsite bootstrap signature */
    public final static MethodType DEF_BOOTSTRAP_TYPE =
        MethodType.methodType(CallSite.class, MethodHandles.Lookup.class, String.class, MethodType.class, int.class, long.class);
    public final static Handle DEF_BOOTSTRAP_HANDLE =
        new Handle(Opcodes.H_INVOKESTATIC, Type.getInternalName(DefBootstrap.class),
            "bootstrap", DEF_BOOTSTRAP_TYPE.toMethodDescriptorString());

    public final static Type DEF_UTIL_TYPE = Type.getType(Def.class);
    public final static Method DEF_TO_BOOLEAN         = getAsmMethod(boolean.class, "DefToboolean"       , Object.class);
    public final static Method DEF_TO_BYTE_IMPLICIT   = getAsmMethod(byte.class   , "DefTobyteImplicit"  , Object.class);
    public final static Method DEF_TO_SHORT_IMPLICIT  = getAsmMethod(short.class  , "DefToshortImplicit" , Object.class);
    public final static Method DEF_TO_CHAR_IMPLICIT   = getAsmMethod(char.class   , "DefTocharImplicit"  , Object.class);
    public final static Method DEF_TO_INT_IMPLICIT    = getAsmMethod(int.class    , "DefTointImplicit"   , Object.class);
    public final static Method DEF_TO_LONG_IMPLICIT   = getAsmMethod(long.class   , "DefTolongImplicit"  , Object.class);
    public final static Method DEF_TO_FLOAT_IMPLICIT  = getAsmMethod(float.class  , "DefTofloatImplicit" , Object.class);
    public final static Method DEF_TO_DOUBLE_IMPLICIT = getAsmMethod(double.class , "DefTodoubleImplicit", Object.class);
    public final static Method DEF_TO_BYTE_EXPLICIT   = getAsmMethod(byte.class   , "DefTobyteExplicit"  , Object.class);
    public final static Method DEF_TO_SHORT_EXPLICIT  = getAsmMethod(short.class  , "DefToshortExplicit" , Object.class);
    public final static Method DEF_TO_CHAR_EXPLICIT   = getAsmMethod(char.class   , "DefTocharExplicit"  , Object.class);
    public final static Method DEF_TO_INT_EXPLICIT    = getAsmMethod(int.class    , "DefTointExplicit"   , Object.class);
    public final static Method DEF_TO_LONG_EXPLICIT   = getAsmMethod(long.class   , "DefTolongExplicit"  , Object.class);
    public final static Method DEF_TO_FLOAT_EXPLICIT  = getAsmMethod(float.class  , "DefTofloatExplicit" , Object.class);
    public final static Method DEF_TO_DOUBLE_EXPLICIT = getAsmMethod(double.class , "DefTodoubleExplicit", Object.class);
    public final static Method DEF_NOT_CALL = getAsmMethod(Object.class , "not", Object.class);
    public final static Method DEF_NEG_CALL = getAsmMethod(Object.class , "neg", Object.class);
    public final static Method DEF_MUL_CALL = getAsmMethod(Object.class , "mul", Object.class, Object.class);
    public final static Method DEF_DIV_CALL = getAsmMethod(Object.class , "div", Object.class, Object.class);
    public final static Method DEF_REM_CALL = getAsmMethod(Object.class , "rem", Object.class, Object.class);
    public final static Method DEF_ADD_CALL = getAsmMethod(Object.class , "add", Object.class, Object.class);
    public final static Method DEF_SUB_CALL = getAsmMethod(Object.class , "sub", Object.class, Object.class);
    public final static Method DEF_LSH_CALL = getAsmMethod(Object.class , "lsh", Object.class, int.class);
    public final static Method DEF_RSH_CALL = getAsmMethod(Object.class , "rsh", Object.class, int.class);
    public final static Method DEF_USH_CALL = getAsmMethod(Object.class , "ush", Object.class, int.class);
    public final static Method DEF_AND_CALL = getAsmMethod(Object.class , "and", Object.class, Object.class);
    public final static Method DEF_XOR_CALL = getAsmMethod(Object.class , "xor", Object.class, Object.class);
    public final static Method DEF_OR_CALL  = getAsmMethod(Object.class , "or" , Object.class, Object.class);
    public final static Method DEF_EQ_CALL  = getAsmMethod(boolean.class, "eq" , Object.class, Object.class);
    public final static Method DEF_LT_CALL  = getAsmMethod(boolean.class, "lt" , Object.class, Object.class);
    public final static Method DEF_LTE_CALL = getAsmMethod(boolean.class, "lte", Object.class, Object.class);
    public final static Method DEF_GT_CALL  = getAsmMethod(boolean.class, "gt" , Object.class, Object.class);
    public final static Method DEF_GTE_CALL = getAsmMethod(boolean.class, "gte", Object.class, Object.class);
    
    /** invokedynamic bootstrap for lambda expression/method references */
    public final static MethodType LAMBDA_BOOTSTRAP_TYPE =
            MethodType.methodType(CallSite.class, MethodHandles.Lookup.class, String.class, 
                                  MethodType.class, Object[].class);
    public final static Handle LAMBDA_BOOTSTRAP_HANDLE =
            new Handle(Opcodes.H_INVOKESTATIC, Type.getInternalName(LambdaMetafactory.class),
                "altMetafactory", LAMBDA_BOOTSTRAP_TYPE.toMethodDescriptorString());
        
    /** dynamic invokedynamic bootstrap for indy string concats (Java 9+) */
    public final static Handle INDY_STRING_CONCAT_BOOTSTRAP_HANDLE;
    static {
        Handle bs;
        try {
            final Class<?> factory = Class.forName("java.lang.invoke.StringConcatFactory");
            final String methodName = "makeConcat";
            final MethodType type = MethodType.methodType(CallSite.class, MethodHandles.Lookup.class, String.class, MethodType.class);
            // ensure it is there:
            MethodHandles.publicLookup().findStatic(factory, methodName, type);
            bs = new Handle(Opcodes.H_INVOKESTATIC, Type.getInternalName(factory), methodName, type.toMethodDescriptorString());
        } catch (ReflectiveOperationException e) {
            // not Java 9 - we set it null, so MethodWriter uses StringBuilder:
            bs = null;
        }
        INDY_STRING_CONCAT_BOOTSTRAP_HANDLE = bs;
    }

    public final static int MAX_INDY_STRING_CONCAT_ARGS = 200;

    public final static Type STRING_TYPE = Type.getType(String.class);
    public final static Type STRINGBUILDER_TYPE = Type.getType(StringBuilder.class);

    public final static Method STRINGBUILDER_CONSTRUCTOR    = getAsmMethod(void.class, "<init>");
    public final static Method STRINGBUILDER_APPEND_BOOLEAN = getAsmMethod(StringBuilder.class, "append", boolean.class);
    public final static Method STRINGBUILDER_APPEND_CHAR    = getAsmMethod(StringBuilder.class, "append", char.class);
    public final static Method STRINGBUILDER_APPEND_INT     = getAsmMethod(StringBuilder.class, "append", int.class);
    public final static Method STRINGBUILDER_APPEND_LONG    = getAsmMethod(StringBuilder.class, "append", long.class);
    public final static Method STRINGBUILDER_APPEND_FLOAT   = getAsmMethod(StringBuilder.class, "append", float.class);
    public final static Method STRINGBUILDER_APPEND_DOUBLE  = getAsmMethod(StringBuilder.class, "append", double.class);
    public final static Method STRINGBUILDER_APPEND_STRING  = getAsmMethod(StringBuilder.class, "append", String.class);
    public final static Method STRINGBUILDER_APPEND_OBJECT  = getAsmMethod(StringBuilder.class, "append", Object.class);
    public final static Method STRINGBUILDER_TOSTRING       = getAsmMethod(String.class, "toString");

    public final static Method CHECKEQUALS = getAsmMethod(boolean.class, "checkEquals", Object.class, Object.class);

    private static Method getAsmMethod(final Class<?> rtype, final String name, final Class<?>... ptypes) {
        return new Method(name, MethodType.methodType(rtype, ptypes).toMethodDescriptorString());
    }

    private WriterConstants() {}
}
