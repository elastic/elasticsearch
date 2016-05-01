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

package org.elasticsearch.painless.tree.walker.analyzer;

import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Transform;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.tree.node.Node;
import static org.elasticsearch.painless.tree.node.Type.ACONSTANT;
import static org.elasticsearch.painless.tree.node.Type.ACAST;
import static org.elasticsearch.painless.tree.node.Type.ATRANSFORM;

import java.lang.reflect.InvocationTargetException;

class AnalyzerCaster {
    private final Definition definition;

    AnalyzerCaster(final Definition definition) {
        this.definition = definition;
    }

    Node markCast(final Node node, final MetadataExpression me) {
        if (me.actual == null || me.expected == null) {
            throw new IllegalStateException(node.error("Illegal cast."));
        }

        final Cast cast = getLegalCast(node, me.actual, me.expected, me.explicit || !me.typesafe);

        if (cast == null) {
            if (me.constant == null) {
                node.data.put("type", me.actual);

                return node;
            } else {
                final Node rtn = new Node(node.location, ACONSTANT);
                rtn.data.put("type", me.actual);
                rtn.data.put("constant", me.constant);

                return rtn;
            }
        } else {
            final Node child;

            if (me.constant == null) {
                child = node;
            } else {
                Object constant = me.constant;

                if (me.expected.sort.constant) {
                    constant = constCast(node, me.constant, cast);
                }

                child = new Node(node.location, ACONSTANT);
                child.data.put("constant", constant);
            }

            child.data.put("type", me.actual);

            final Node rtn = new Node(node.location, cast instanceof Transform ? ATRANSFORM : ACAST);
            rtn.data.put("type", me.expected);
            rtn.data.put("cast", cast);
            rtn.children.add(child);

            return rtn;
        }
    }

    Node markCast(final Node node, final Type before, final Type after, final boolean explicit) {
        final Cast cast = getLegalCast(node, before, after, explicit);

        if (cast == null) {
            return null;
        } else {
            final Node rtn = new Node(node.location, cast instanceof Transform ? ATRANSFORM : ACAST);
            rtn.data.put("cast", cast);
            rtn.children.add(node);

            return rtn;
        }
    }

    Cast getLegalCast(final Node node, final Type actual, final Type expected, final boolean explicit) {
        final Cast cast = new Cast(actual, expected);

        if (actual.equals(expected)) {
            return null;
        }

        if (actual.sort == Sort.DEF && expected.sort != Sort.VOID || actual.sort != Sort.VOID && expected.sort == Sort.DEF) {
            final Transform transform = definition.transforms.get(cast);

            if (transform != null) {
                return transform;
            }

            return cast;
        }

        switch (actual.sort) {
            case BOOL:
                switch (expected.sort) {
                    case OBJECT:
                    case BOOL_OBJ:
                        return checkTransform(node, cast);
                }

                break;
            case BYTE:
                switch (expected.sort) {
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
                        return checkTransform(node, cast);
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case SHORT:
                switch (expected.sort) {
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
                        return checkTransform(node, cast);
                    case BYTE_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case CHAR:
                switch (expected.sort) {
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
                        return checkTransform(node, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case INT:
                switch (expected.sort) {
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
                        return checkTransform(node, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case LONG:
                switch (expected.sort) {
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
                        return checkTransform(node, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case FLOAT:
                switch (expected.sort) {
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
                        return checkTransform(node, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case DOUBLE:
                switch (expected.sort) {
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
                        return checkTransform(node, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case OBJECT:
            case NUMBER:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case BOOL_OBJ:
                switch (expected.sort) {
                    case BOOL:
                        return checkTransform(node, cast);
                }

                break;
            case BYTE_OBJ:
                switch (expected.sort) {
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
                        return checkTransform(node, cast);
                    case CHAR:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case SHORT_OBJ:
                switch (expected.sort) {
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(node, cast);
                    case BYTE:
                    case CHAR:
                    case BYTE_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case CHAR_OBJ:
                switch (expected.sort) {
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(node, cast);
                    case BYTE:
                    case SHORT:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case INT_OBJ:
                switch (expected.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(node, cast);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case LONG_OBJ:
                switch (expected.sort) {
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(node, cast);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                        if (explicit)
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case FLOAT_OBJ:
                switch (expected.sort) {
                    case FLOAT:
                    case DOUBLE:
                    case DOUBLE_OBJ:
                        return checkTransform(node, cast);
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
                            return checkTransform(node, cast);

                        break;
                }

                break;
            case DOUBLE_OBJ:
                switch (expected.sort) {
                    case DOUBLE:
                        return checkTransform(node, cast);
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
                            return checkTransform(node, cast);

                        break;
                }

                break;
        }

        try {
            actual.clazz.asSubclass(expected.clazz);

            return cast;
        } catch (final ClassCastException cce0) {
            try {
                if (explicit) {
                    expected.clazz.asSubclass(actual.clazz);

                    return cast;
                } else {
                    throw new ClassCastException(node.error("Cannot cast from [" + actual.name + "] to [" + expected.name + "]."));
                }
            } catch (final ClassCastException cce1) {
                throw new ClassCastException(node.error("Cannot cast from [" + actual.name + "] to [" + expected.name + "]."));
            }
        }
    }

    private Transform checkTransform(final Node node, final Cast cast) {
        final Transform transform = definition.transforms.get(cast);

        if (transform == null) {
            throw new ClassCastException(node.error("Cannot cast from [" + cast.from.name + "] to [" + cast.to.name + "]."));
        }

        return transform;
    }

    private Object constCast(final Node node, final Object constant, final Cast cast) {
        if (cast instanceof Transform) {
            final Transform transform = (Transform)cast;
            return invokeTransform(node, transform, constant);
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
                        throw new IllegalStateException(node.error("Cannot cast from " +
                            "[" + cast.from.clazz.getCanonicalName() + "] to [" + cast.to.clazz.getCanonicalName() + "]."));
                }
            } else {
                throw new IllegalStateException(node.error("Cannot cast from " +
                    "[" + cast.from.clazz.getCanonicalName() + "] to [" + cast.to.clazz.getCanonicalName() + "]."));
            }
        }
    }

    private Object invokeTransform(final Node node, final Transform transform, final Object object) {
        final Method method = transform.method;
        final java.lang.reflect.Method jmethod = method.reflect;
        final int modifiers = jmethod.getModifiers();

        try {
            if (java.lang.reflect.Modifier.isStatic(modifiers)) {
                return jmethod.invoke(null, object);
            } else {
                return jmethod.invoke(object);
            }
        } catch (final IllegalAccessException | IllegalArgumentException |
            InvocationTargetException | NullPointerException | ExceptionInInitializerError exception) {
            throw new IllegalStateException(node.error("Cannot cast from [" + transform.from.name + "] to [" + transform.to.name + "]."));
        }
    }
}

