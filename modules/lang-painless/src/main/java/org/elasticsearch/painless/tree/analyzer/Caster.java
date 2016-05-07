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

package org.elasticsearch.painless.tree.analyzer;

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Pair;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Transform;
import org.elasticsearch.painless.Definition.Type;

import java.lang.reflect.InvocationTargetException;

public class Caster {
    public static Cast getLegalCast(final Definition definition,
                                    final String location, final Type actual, final Type expected, final boolean explicit) {
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
                        return checkTransform(definition, location, cast);
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
                        return checkTransform(definition, location, cast);
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(definition, location, cast);

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
                        return checkTransform(definition, location, cast);
                    case BYTE_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(definition, location, cast);

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
                        return checkTransform(definition, location, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case STRING:
                        if (explicit)
                            return checkTransform(definition, location, cast);

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
                        return checkTransform(definition, location, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(definition, location, cast);

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
                        return checkTransform(definition, location, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                        if (explicit)
                            return checkTransform(definition, location, cast);

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
                        return checkTransform(definition, location, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                        if (explicit)
                            return checkTransform(definition, location, cast);

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
                        return checkTransform(definition, location, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                        if (explicit)
                            return checkTransform(definition, location, cast);

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
                            return checkTransform(definition, location, cast);

                        break;
                }

                break;
            case BOOL_OBJ:
                switch (expected.sort) {
                    case BOOL:
                        return checkTransform(definition, location, cast);
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
                        return checkTransform(definition, location, cast);
                    case CHAR:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(definition, location, cast);

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
                        return checkTransform(definition, location, cast);
                    case BYTE:
                    case CHAR:
                    case BYTE_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(definition, location, cast);

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
                        return checkTransform(definition, location, cast);
                    case BYTE:
                    case SHORT:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case STRING:
                        if (explicit)
                            return checkTransform(definition, location, cast);

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
                        return checkTransform(definition, location, cast);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(definition, location, cast);

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
                        return checkTransform(definition, location, cast);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                        if (explicit)
                            return checkTransform(definition, location, cast);

                        break;
                }

                break;
            case FLOAT_OBJ:
                switch (expected.sort) {
                    case FLOAT:
                    case DOUBLE:
                    case DOUBLE_OBJ:
                        return checkTransform(definition, location, cast);
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
                            return checkTransform(definition, location, cast);

                        break;
                }

                break;
            case DOUBLE_OBJ:
                switch (expected.sort) {
                    case DOUBLE:
                        return checkTransform(definition, location, cast);
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
                            return checkTransform(definition, location, cast);

                        break;
                }

                break;
            case STRING:
                switch (expected.sort) {
                    case CHAR:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(definition, location, cast);

                        break;
                }
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
                    throw new ClassCastException(
                        "Error" + location + ": Cannot cast from [" + actual.name + "] to [" + expected.name + "].");
                }
            } catch (final ClassCastException cce1) {
                throw new ClassCastException("Error" + location + ": Cannot cast from [" + actual.name + "] to [" + expected.name + "].");
            }
        }
    }

    private static Transform checkTransform(final Definition definition, final String location, final Cast cast) {
        final Transform transform = definition.transforms.get(cast);

        if (transform == null) {
            throw new ClassCastException("Error" + location + ": Cannot cast from [" + cast.from.name + "] to [" + cast.to.name + "].");
        }

        return transform;
    }

    public static Object constCast(final String location, final Object constant, final Cast cast) {
        if (cast instanceof Transform) {
            final Transform transform = (Transform)cast;
            return invokeTransform(location, transform, constant);
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
                        throw new IllegalStateException("Error" + location + ": Cannot cast from " +
                            "[" + cast.from.clazz.getCanonicalName() + "] to [" + cast.to.clazz.getCanonicalName() + "].");
                }
            } else {
                throw new IllegalStateException("Error" + location + ": Cannot cast from " +
                    "[" + cast.from.clazz.getCanonicalName() + "] to [" + cast.to.clazz.getCanonicalName() + "].");
            }
        }
    }

    private static Object invokeTransform(final String location, final Transform transform, final Object object) {
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
            throw new ClassCastException(
                "Error" + location + ": Cannot cast from [" + transform.from.name + "] to [" + transform.to.name + "].");
        }
    }

    public static Type promoteNumeric(final Definition definition, final Type from, final boolean decimal, final boolean primitive) {
        final Sort sort = from.sort;

        if (sort == Sort.DEF) {
            return definition.defType;
        } else if ((sort == Sort.DOUBLE || sort == Sort.DOUBLE_OBJ) && decimal) {
            return primitive ? definition.doubleType : definition.doubleobjType;
        } else if ((sort == Sort.FLOAT || sort == Sort.FLOAT_OBJ) && decimal) {
            return primitive ? definition.floatType : definition.floatobjType;
        } else if (sort == Sort.LONG || sort == Sort.LONG_OBJ) {
            return primitive ? definition.longType : definition.longobjType;
        } else if (sort == Sort.INT   || sort == Sort.INT_OBJ   ||
                   sort == Sort.CHAR  || sort == Sort.CHAR_OBJ  ||
                   sort == Sort.SHORT || sort == Sort.SHORT_OBJ ||
                   sort == Sort.BYTE  || sort == Sort.BYTE_OBJ) {
            return primitive ? definition.intType : definition.intobjType;
        }

        return null;
    }

    public static Type promoteNumeric(final Definition definition,
                                      final Type from0, final Type from1, final boolean decimal, final boolean primitive) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return definition.defType;
        }

        if (decimal) {
            if (sort0 == Sort.DOUBLE || sort0 == Sort.DOUBLE_OBJ ||
                sort1 == Sort.DOUBLE || sort1 == Sort.DOUBLE_OBJ) {
                return primitive ? definition.doubleType : definition.doubleobjType;
            } else if (sort0 == Sort.FLOAT || sort0 == Sort.FLOAT_OBJ || sort1 == Sort.FLOAT || sort1 == Sort.FLOAT_OBJ) {
                return primitive ? definition.floatType : definition.floatobjType;
            }
        }

        if (sort0 == Sort.LONG || sort0 == Sort.LONG_OBJ ||
            sort1 == Sort.LONG || sort1 == Sort.LONG_OBJ) {
            return primitive ? definition.longType : definition.longobjType;
        } else if (sort0 == Sort.INT   || sort0 == Sort.INT_OBJ   ||
                   sort1 == Sort.INT   || sort1 == Sort.INT_OBJ   ||
                   sort0 == Sort.CHAR  || sort0 == Sort.CHAR_OBJ  ||
                   sort1 == Sort.CHAR  || sort1 == Sort.CHAR_OBJ  ||
                   sort0 == Sort.SHORT || sort0 == Sort.SHORT_OBJ ||
                   sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ ||
                   sort0 == Sort.BYTE  || sort0 == Sort.BYTE_OBJ  ||
                   sort1 == Sort.BYTE  || sort1 == Sort.BYTE_OBJ) {
            return primitive ? definition.intType : definition.intobjType;
        }

        return null;
    }

    public static Type promoteAdd(final Definition definition, final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.STRING || sort1 == Sort.STRING) {
            return definition.stringType;
        }

        return promoteNumeric(definition, from0, from1, true, true);
    }

    public static Type promoteXor(final Definition definition, final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0.bool || sort1.bool) {
            return definition.booleanType;
        }

        return promoteNumeric(definition, from0, from1, false, true);
    }

    public static Type promoteEquality(final Definition definition, final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return definition.defType;
        }

        final boolean primitive = sort0.primitive && sort1.primitive;

        if (sort0.bool && sort1.bool) {
            return primitive ? definition.booleanType : definition.booleanobjType;
        }

        if (sort0.numeric && sort1.numeric) {
            return promoteNumeric(definition, from0, from1, true, primitive);
        }

        return definition.objectType;
    }

    public static Type promoteReference(final Definition definition, final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return definition.defType;
        }

        if (sort0.primitive && sort1.primitive) {
            if (sort0.bool && sort1.bool) {
                return definition.booleanType;
            }

            if (sort0.numeric && sort1.numeric) {
                return promoteNumeric(definition, from0, from1, true, true);
            }
        }

        return definition.objectType;
    }

    public static Type promoteConditional(final Definition definition,
                                          final Type from0, final Type from1, final Object const0, final Object const1) {
        if (from0.equals(from1)) {
            return from0;
        }

        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return definition.defType;
        }

        final boolean primitive = sort0.primitive && sort1.primitive;

        if (sort0.bool && sort1.bool) {
            return primitive ? definition.booleanType : definition.booleanobjType;
        }

        if (sort0.numeric && sort1.numeric) {
            if (sort0 == Sort.DOUBLE || sort0 == Sort.DOUBLE_OBJ || sort1 == Sort.DOUBLE || sort1 == Sort.DOUBLE_OBJ) {
                return primitive ? definition.doubleType : definition.doubleobjType;
            } else if (sort0 == Sort.FLOAT || sort0 == Sort.FLOAT_OBJ || sort1 == Sort.FLOAT || sort1 == Sort.FLOAT_OBJ) {
                return primitive ? definition.floatType : definition.floatobjType;
            } else if (sort0 == Sort.LONG || sort0 == Sort.LONG_OBJ || sort1 == Sort.LONG || sort1 == Sort.LONG_OBJ) {
                return sort0.primitive && sort1.primitive ? definition.longType : definition.longobjType;
            } else {
                if (sort0 == Sort.BYTE || sort0 == Sort.BYTE_OBJ) {
                    if (sort1 == Sort.BYTE || sort1 == Sort.BYTE_OBJ) {
                        return primitive ? definition.byteType : definition.byteobjType;
                    } else if (sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ) {
                        if (const1 != null) {
                            final short constant = (short)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.shortType : definition.shortobjType;
                    } else if (sort1 == Sort.CHAR || sort1 == Sort.CHAR_OBJ) {
                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.INT || sort1 == Sort.INT_OBJ) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.intType : definition.intobjType;
                    }
                } else if (sort0 == Sort.SHORT || sort0 == Sort.SHORT_OBJ) {
                    if (sort1 == Sort.BYTE || sort1 == Sort.BYTE_OBJ) {
                        if (const0 != null) {
                            final short constant = (short)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.shortType : definition.shortobjType;
                    } else if (sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ) {
                        return primitive ? definition.shortType : definition.shortobjType;
                    } else if (sort1 == Sort.CHAR || sort1 == Sort.CHAR_OBJ) {
                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.INT || sort1 == Sort.INT_OBJ) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return primitive ? definition.shortType : definition.shortobjType;
                            }
                        }

                        return primitive ? definition.intType : definition.intobjType;
                    }
                } else if (sort0 == Sort.CHAR || sort0 == Sort.CHAR_OBJ) {
                    if (sort1 == Sort.BYTE || sort1 == Sort.BYTE_OBJ) {
                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ) {
                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.CHAR || sort1 == Sort.CHAR_OBJ) {
                        return primitive ? definition.charType : definition.charobjType;
                    } else if (sort1 == Sort.INT || sort1 == Sort.INT_OBJ) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.intType : definition.intobjType;
                    }
                } else if (sort0 == Sort.INT || sort0 == Sort.INT_OBJ) {
                    if (sort1 == Sort.BYTE || sort1 == Sort.BYTE_OBJ) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.CHAR || sort1 == Sort.CHAR_OBJ) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.INT || sort1 == Sort.INT_OBJ) {
                        return primitive ? definition.intType : definition.intobjType;
                    }
                }
            }
        }

        final Pair pair = new Pair(from0, from1);
        final Type bound = definition.bounds.get(pair);

        return bound == null ? definition.objectType : bound;
    }
}
