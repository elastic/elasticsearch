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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;

public class WriterConstants {
    public final static String BASE_CLASS_NAME = Executable.class.getName();
    public final static String CLASS_NAME      = BASE_CLASS_NAME + "$CompiledPainlessExecutable";
    public final static Type BASE_CLASS_TYPE   = Type.getType(Executable.class);
    public final static Type CLASS_TYPE        = Type.getType("L" + CLASS_NAME.replace(".", "/") + ";");

    final static Method CONSTRUCTOR = getAsmMethod(void.class, "<init>", String.class, String.class);
    final static Method EXECUTE     = getAsmMethod(Object.class, "execute", Map.class, Scorer.class, LeafDocLookup.class);

    public final static Type PAINLESS_ERROR_TYPE = Type.getType(PainlessError.class);

    public final static Type NEEDS_SCORE_TYPE = Type.getType(NeedsScore.class);
    public final static Type SCORER_TYPE = Type.getType(Scorer.class);
    public final static Method SCORER_SCORE = getAsmMethod(float.class, "score");

    public final static Type MAP_TYPE  = Type.getType(Map.class);
    public final static Method MAP_GET = getAsmMethod(Object.class, "get", Object.class);

    /** dynamic callsite bootstrap signature */
    public final static MethodType DEF_BOOTSTRAP_TYPE =
        MethodType.methodType(CallSite.class, MethodHandles.Lookup.class, String.class, MethodType.class, int.class);
    public final static Handle DEF_BOOTSTRAP_HANDLE =
        new Handle(Opcodes.H_INVOKESTATIC, Type.getInternalName(DynamicCallSite.class),
            "bootstrap", DEF_BOOTSTRAP_TYPE.toMethodDescriptorString());

    public final static String DEF_DYNAMIC_LOAD_FIELD_DESC =
        MethodType.methodType(Object.class, Object.class).toMethodDescriptorString();
    public final static String DEF_DYNAMIC_STORE_FIELD_DESC =
        MethodType.methodType(void.class, Object.class, Object.class).toMethodDescriptorString();
    public final static String DEF_DYNAMIC_ARRAY_LOAD_DESC =
        MethodType.methodType(Object.class, Object.class, Object.class).toMethodDescriptorString();
    public final static String DEF_DYNAMIC_ARRAY_STORE_DESC =
        MethodType.methodType(void.class, Object.class, Object.class, Object.class).toMethodDescriptorString();

    public final static Method DEF_NOT_CALL = getAsmMethod(Object.class, "not", Object.class);
    public final static Method DEF_NEG_CALL = getAsmMethod(Object.class, "neg", Object.class);
    public final static Method DEF_MUL_CALL = getAsmMethod(Object.class, "mul", Object.class, Object.class);
    public final static Method DEF_DIV_CALL = getAsmMethod(Object.class, "div", Object.class, Object.class);
    public final static Method DEF_REM_CALL = getAsmMethod(Object.class, "rem", Object.class, Object.class);
    public final static Method DEF_ADD_CALL = getAsmMethod(Object.class, "add", Object.class, Object.class);
    public final static Method DEF_SUB_CALL = getAsmMethod(Object.class, "sub", Object.class, Object.class);
    public final static Method DEF_LSH_CALL = getAsmMethod(Object.class, "lsh", Object.class, int.class);
    public final static Method DEF_RSH_CALL = getAsmMethod(Object.class, "rsh", Object.class, int.class);
    public final static Method DEF_USH_CALL = getAsmMethod(Object.class, "ush", Object.class, int.class);
    public final static Method DEF_AND_CALL = getAsmMethod(Object.class, "and", Object.class, Object.class);
    public final static Method DEF_XOR_CALL = getAsmMethod(Object.class, "xor", Object.class, Object.class);
    public final static Method DEF_OR_CALL  = getAsmMethod(Object.class, "or" , Object.class, Object.class);
    public final static Method DEF_EQ_CALL  = getAsmMethod(boolean.class, "eq" , Object.class, Object.class);
    public final static Method DEF_LT_CALL  = getAsmMethod(boolean.class, "lt" , Object.class, Object.class);
    public final static Method DEF_LTE_CALL = getAsmMethod(boolean.class, "lte", Object.class, Object.class);
    public final static Method DEF_GT_CALL  = getAsmMethod(boolean.class, "gt" , Object.class, Object.class);
    public final static Method DEF_GTE_CALL = getAsmMethod(boolean.class, "gte", Object.class, Object.class);

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

    public final static Method TOINTEXACT_LONG  = getAsmMethod(int.class,  "toIntExact",    long.class);
    public final static Method NEGATEEXACT_INT  = getAsmMethod(int.class,  "negateExact",   int.class);
    public final static Method NEGATEEXACT_LONG = getAsmMethod(long.class, "negateExact",   long.class);
    public final static Method MULEXACT_INT     = getAsmMethod(int.class,  "multiplyExact", int.class,  int.class);
    public final static Method MULEXACT_LONG    = getAsmMethod(long.class, "multiplyExact", long.class, long.class);
    public final static Method ADDEXACT_INT     = getAsmMethod(int.class,  "addExact",      int.class,  int.class);
    public final static Method ADDEXACT_LONG    = getAsmMethod(long.class, "addExact",      long.class, long.class);
    public final static Method SUBEXACT_INT     = getAsmMethod(int.class,  "subtractExact", int.class,  int.class);
    public final static Method SUBEXACT_LONG    = getAsmMethod(long.class, "subtractExact", long.class, long.class);

    public final static Method CHECKEQUALS              =
        getAsmMethod(boolean.class, "checkEquals",              Object.class, Object.class);
    public final static Method TOBYTEEXACT_INT          = getAsmMethod(byte.class,    "toByteExact",              int.class);
    public final static Method TOBYTEEXACT_LONG         = getAsmMethod(byte.class,    "toByteExact",              long.class);
    public final static Method TOBYTEWOOVERFLOW_FLOAT   = getAsmMethod(byte.class,    "toByteWithoutOverflow",    float.class);
    public final static Method TOBYTEWOOVERFLOW_DOUBLE  = getAsmMethod(byte.class,    "toByteWithoutOverflow",    double.class);
    public final static Method TOSHORTEXACT_INT         = getAsmMethod(short.class,   "toShortExact",             int.class);
    public final static Method TOSHORTEXACT_LONG        = getAsmMethod(short.class,   "toShortExact",             long.class);
    public final static Method TOSHORTWOOVERFLOW_FLOAT  = getAsmMethod(short.class,   "toShortWithoutOverflow",   float.class);
    public final static Method TOSHORTWOOVERFLOW_DOUBLE = getAsmMethod(short.class,   "toShortWihtoutOverflow",   double.class);
    public final static Method TOCHAREXACT_INT          = getAsmMethod(char.class,    "toCharExact",              int.class);
    public final static Method TOCHAREXACT_LONG         = getAsmMethod(char.class,    "toCharExact",              long.class);
    public final static Method TOCHARWOOVERFLOW_FLOAT   = getAsmMethod(char.class,    "toCharWithoutOverflow",    float.class);
    public final static Method TOCHARWOOVERFLOW_DOUBLE  = getAsmMethod(char.class,    "toCharWithoutOverflow",    double.class);
    public final static Method TOINTWOOVERFLOW_FLOAT    = getAsmMethod(int.class,     "toIntWithoutOverflow",     float.class);
    public final static Method TOINTWOOVERFLOW_DOUBLE   = getAsmMethod(int.class,     "toIntWithoutOverflow",     double.class);
    public final static Method TOLONGWOOVERFLOW_FLOAT   = getAsmMethod(long.class,    "toLongWithoutOverflow",    float.class);
    public final static Method TOLONGWOOVERFLOW_DOUBLE  = getAsmMethod(long.class,    "toLongWithoutOverflow",    double.class);
    public final static Method TOFLOATWOOVERFLOW_DOUBLE = getAsmMethod(float.class ,  "toFloatWihtoutOverflow",   double.class);
    public final static Method MULWOOVERLOW_FLOAT       =
        getAsmMethod(float.class,   "multiplyWithoutOverflow",  float.class,  float.class);
    public final static Method MULWOOVERLOW_DOUBLE      =
        getAsmMethod(double.class,  "multiplyWithoutOverflow",  double.class, double.class);
    public final static Method DIVWOOVERLOW_INT         =
        getAsmMethod(int.class,     "divideWithoutOverflow",    int.class,    int.class);
    public final static Method DIVWOOVERLOW_LONG        =
        getAsmMethod(long.class,    "divideWithoutOverflow",    long.class,   long.class);
    public final static Method DIVWOOVERLOW_FLOAT       =
        getAsmMethod(float.class,   "divideWithoutOverflow",    float.class,  float.class);
    public final static Method DIVWOOVERLOW_DOUBLE      =
        getAsmMethod(double.class,  "divideWithoutOverflow",    double.class, double.class);
    public final static Method REMWOOVERLOW_FLOAT       =
        getAsmMethod(float.class,   "remainderWithoutOverflow", float.class,  float.class);
    public final static Method REMWOOVERLOW_DOUBLE      =
        getAsmMethod(double.class,  "remainderWithoutOverflow", double.class, double.class);
    public final static Method ADDWOOVERLOW_FLOAT       =
        getAsmMethod(float.class,   "addWithoutOverflow",       float.class,  float.class);
    public final static Method ADDWOOVERLOW_DOUBLE      =
        getAsmMethod(double.class,  "addWithoutOverflow",       double.class, double.class);
    public final static Method SUBWOOVERLOW_FLOAT       =
        getAsmMethod(float.class,   "subtractWithoutOverflow",  float.class,  float.class);
    public final static Method SUBWOOVERLOW_DOUBLE      =
        getAsmMethod(double.class,  "subtractWithoutOverflow",  double.class, double.class);

    private static Method getAsmMethod(final Class<?> rtype, final String name, final Class<?>... ptypes) {
        return new Method(name, MethodType.methodType(rtype, ptypes).toMethodDescriptorString());
    }

    private WriterConstants() {}
}
