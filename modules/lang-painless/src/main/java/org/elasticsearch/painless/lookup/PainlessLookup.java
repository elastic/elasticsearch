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

package org.elasticsearch.painless.lookup;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistClass;
import org.elasticsearch.painless.spi.WhitelistConstructor;
import org.elasticsearch.painless.spi.WhitelistField;
import org.elasticsearch.painless.spi.WhitelistMethod;
import org.objectweb.asm.Type;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * The entire API for Painless.  Also used as a whitelist for checking for legal
 * methods and fields during at both compile-time and runtime.
 */
public final class PainlessLookup extends PainlessLookupBase {

    public static Class<?> getBoxedType(Class<?> clazz) {
        if (clazz == boolean.class) {
            return Boolean.class;
        } else if (clazz == byte.class) {
            return Byte.class;
        } else if (clazz == short.class) {
            return Short.class;
        } else if (clazz == char.class) {
            return Character.class;
        } else if (clazz == int.class) {
            return Integer.class;
        } else if (clazz == long.class) {
            return Long.class;
        } else if (clazz == float.class) {
            return Float.class;
        } else if (clazz == double.class) {
            return Double.class;
        }

        return clazz;
    }

    public static Class<?> getUnboxedype(Class<?> clazz) {
        if (clazz == Boolean.class) {
            return boolean.class;
        } else if (clazz == Byte.class) {
            return byte.class;
        } else if (clazz == Short.class) {
            return short.class;
        } else if (clazz == Character.class) {
            return char.class;
        } else if (clazz == Integer.class) {
            return int.class;
        } else if (clazz == Long.class) {
            return long.class;
        } else if (clazz == Float.class) {
            return float.class;
        } else if (clazz == Double.class) {
            return double.class;
        }

        return clazz;
    }

    public static boolean isConstantType(Class<?> clazz) {
        return clazz == boolean.class ||
               clazz == byte.class    ||
               clazz == short.class   ||
               clazz == char.class    ||
               clazz == int.class     ||
               clazz == long.class    ||
               clazz == float.class   ||
               clazz == double.class  ||
               clazz == String.class;
    }

    public Class<?> getClassFromBinaryName(String painlessType) {
        return painlessTypesToJavaClasses.get(painlessType.replace('$', '.'));
    }

    public static String ClassToName(Class<?> clazz) {
        if (clazz.isLocalClass() || clazz.isAnonymousClass()) {
            return null;
        } else if (clazz.isArray()) {
            Class<?> component = clazz.getComponentType();
            int dimensions = 1;

            while (component.isArray()) {
                component = component.getComponentType();
                ++dimensions;
            }

            if (component == def.class) {
                StringBuilder builder = new StringBuilder(def.class.getSimpleName());

                for (int dimension = 0; dimension < dimensions; dimension++) {
                    builder.append("[]");
                }

                return builder.toString();
            }
        } else if (clazz == def.class) {
            return def.class.getSimpleName();
        }

        return clazz.getCanonicalName().replace('$', '.');
    }

    public Collection<PainlessClass> getStructs() {
        return javaClassesToPainlessStructs.values();
    }

    PainlessLookup(Map<String, Class<?>> painlessClassNamesToJavaClasses, Map<Class<?>, PainlessClass> javaClassesToPainlessClasses) {
        super(painlessClassNamesToJavaClasses, javaClassesToPainlessClasses);
    }

    public boolean isSimplePainlessType(String painlessType) {
        return painlessTypesToJavaClasses.containsKey(painlessType);
    }

    public PainlessClass getPainlessStructFromJavaClass(Class<?> clazz) {
        return javaClassesToPainlessStructs.get(clazz);
    }
}
