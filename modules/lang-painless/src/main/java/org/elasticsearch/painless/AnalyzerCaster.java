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

import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Transform;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Metadata.ExpressionMetadata;

class AnalyzerCaster {
    private final Definition definition;

    AnalyzerCaster(final Definition definition) {
        this.definition = definition;
    }

    void markCast(final ExpressionMetadata emd) {
        if (emd.from == null) {
            throw new IllegalStateException(AnalyzerUtility.error(emd.source) + "From cast type should never be null.");
        }

        if (emd.to != null) {
            emd.cast = getLegalCast(emd.source, emd.from, emd.to, emd.explicit || !emd.typesafe);

            if (emd.preConst != null && emd.to.sort.constant) {
                emd.postConst = constCast(emd.source, emd.preConst, emd.cast);
            }
        } else {
            throw new IllegalStateException(AnalyzerUtility.error(emd.source) + "To cast type should never be null.");
        }
    }

    Cast getLegalCast(final ParserRuleContext source, final Type from, final Type to, final boolean explicit) {
        final Cast cast = new Cast(from, to);

        if (from.equals(to)) {
            return cast;
        }

        if (from.sort == Sort.DEF && to.sort != Sort.VOID || from.sort != Sort.VOID && to.sort == Sort.DEF) {
            final Transform transform = definition.transforms.get(cast);

            if (transform != null) {
                return transform;
            }

            return cast;
        }

        switch (from.sort) {
            case BOOL:
                switch (to.sort) {
                    case OBJECT:
                    case BOOL_OBJ:
                        return checkTransform(source, cast);
                }

                break;
            case BYTE:
                switch (to.sort) {
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return cast;
                    case CHAR:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case SHORT:
                switch (to.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return cast;
                    case BYTE:
                    case CHAR:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case SHORT_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case CHAR:
                switch (to.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return cast;
                    case BYTE:
                    case SHORT:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case INT:
                switch (to.sort) {
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return cast;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case LONG:
                switch (to.sort) {
                    case FLOAT:
                    case DOUBLE:
                        return cast;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case FLOAT:
                switch (to.sort) {
                    case DOUBLE:
                        return cast;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case DOUBLE:
                switch (to.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case OBJECT:
            case NUMBER:
                switch (to.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case BOOL_OBJ:
                switch (to.sort) {
                    case BOOL:
                        return checkTransform(source, cast);
                }

                break;
            case BYTE_OBJ:
                switch (to.sort) {
                    case BYTE:
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case SHORT_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case CHAR:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case SHORT_OBJ:
                switch (to.sort) {
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE:
                    case CHAR:
                    case BYTE_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case CHAR_OBJ:
                switch (to.sort) {
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE:
                    case SHORT:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case INT_OBJ:
                switch (to.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case LONG_OBJ:
                switch (to.sort) {
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case FLOAT_OBJ:
                switch (to.sort) {
                    case FLOAT:
                    case DOUBLE:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case DOUBLE_OBJ:
                switch (to.sort) {
                    case DOUBLE:
                        return checkTransform(source, cast);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
        }

        try {
            from.clazz.asSubclass(to.clazz);

            return cast;
        } catch (final ClassCastException cce0) {
            try {
                if (explicit) {
                    to.clazz.asSubclass(from.clazz);

                    return cast;
                } else {
                    throw new ClassCastException(
                        AnalyzerUtility.error(source) + "Cannot cast from [" + from.name + "] to [" + to.name + "].");
                }
            } catch (final ClassCastException cce1) {
                throw new ClassCastException(
                    AnalyzerUtility.error(source) + "Cannot cast from [" + from.name + "] to [" + to.name + "].");
            }
        }
    }

    private Transform checkTransform(final ParserRuleContext source, final Cast cast) {
        final Transform transform = definition.transforms.get(cast);

        if (transform == null) {
            throw new ClassCastException(
                AnalyzerUtility.error(source) + "Cannot cast from [" + cast.from.name + "] to [" + cast.to.name + "].");
        }

        return transform;
    }

    private Object constCast(final ParserRuleContext source, final Object constant, final Cast cast) {
        if (cast instanceof Transform) {
            final Transform transform = (Transform)cast;
            return invokeTransform(source, transform, constant);
        } else {
            final Sort fsort = cast.from.sort;
            final Sort tsort = cast.to.sort;

            if (fsort == tsort) {
                return constant;
            } else if (fsort.numeric && tsort.numeric) {
                Number number;

                if (fsort == Sort.CHAR) {
                    number = (int)(char)constant;
                } else {
                    number = (Number)constant;
                }

                switch (tsort) {
                    case BYTE:   return number.byteValue();
                    case SHORT:  return number.shortValue();
                    case CHAR:   return (char)number.intValue();
                    case INT:    return number.intValue();
                    case LONG:   return number.longValue();
                    case FLOAT:  return number.floatValue();
                    case DOUBLE: return number.doubleValue();
                    default:
                        throw new IllegalStateException(AnalyzerUtility.error(source) + "Expected numeric type for cast.");
                }
            } else {
                throw new IllegalStateException(AnalyzerUtility.error(source) + "No valid constant cast from " +
                    "[" + cast.from.clazz.getCanonicalName() + "] to " +
                    "[" + cast.to.clazz.getCanonicalName() + "].");
            }
        }
    }

    private Object invokeTransform(final ParserRuleContext source, final Transform transform, final Object object) {
        final Method method = transform.method;
        final java.lang.reflect.Method jmethod = method.reflect;
        final int modifiers = jmethod.getModifiers();

        try {
            if (java.lang.reflect.Modifier.isStatic(modifiers)) {
                return jmethod.invoke(null, object);
            } else {
                return jmethod.invoke(object);
            }
        } catch (IllegalAccessException | IllegalArgumentException |
            java.lang.reflect.InvocationTargetException | NullPointerException |
            ExceptionInInitializerError exception) {
            throw new IllegalStateException(AnalyzerUtility.error(source) + "Unable to invoke transform to cast constant from " +
                "[" + transform.from.name + "] to [" + transform.to.name + "].");
        }
    }
}
