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

import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Transform;
import org.elasticsearch.painless.Definition.Type;

import java.lang.reflect.InvocationTargetException;

/**
 * Used during the analysis phase to collect legal type casts and promotions
 * for type-checking and later to write necessary casts in the bytecode.
 */
public final class AnalyzerCaster {

    public static Cast getLegalCast(final String location, final Type actual, final Type expected, final boolean explicit) {
        final Cast cast = new Cast(actual, expected, explicit);

        if (actual.equals(expected)) {
            return null;
        }

        Cast transform = Definition.getTransform(cast);

        if (transform == null && explicit) {
            transform = Definition.getTransform(new Cast(actual, expected, false));
        }

        if (transform != null) {
            return transform;
        }

        if (expected.clazz.isAssignableFrom(actual.clazz) ||
            ((explicit || expected.sort == Sort.DEF) && actual.clazz.isAssignableFrom(expected.clazz))) {
            return cast;
        } else {
            throw new ClassCastException("Error" + location + ": Cannot cast from [" + actual.name + "] to [" + expected.name + "].");
        }
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

    public static Type promoteNumeric(final Type from, final boolean decimal, final boolean primitive) {
        final Sort sort = from.sort;

        if (sort == Sort.DEF) {
            return Definition.defType;
        } else if ((sort == Sort.DOUBLE || sort == Sort.DOUBLE_OBJ) && decimal) {
            return primitive ? Definition.doubleType : Definition.doubleobjType;
        } else if ((sort == Sort.FLOAT || sort == Sort.FLOAT_OBJ) && decimal) {
            return primitive ? Definition.floatType : Definition.floatobjType;
        } else if (sort == Sort.LONG || sort == Sort.LONG_OBJ) {
            return primitive ? Definition.longType : Definition.longobjType;
        } else if (sort == Sort.INT   || sort == Sort.INT_OBJ   ||
                   sort == Sort.CHAR  || sort == Sort.CHAR_OBJ  ||
                   sort == Sort.SHORT || sort == Sort.SHORT_OBJ ||
                   sort == Sort.BYTE  || sort == Sort.BYTE_OBJ) {
            return primitive ? Definition.intType : Definition.intobjType;
        }

        return null;
    }

    public static Type promoteNumeric(final Type from0, final Type from1, final boolean decimal, final boolean primitive) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return Definition.defType;
        }

        if (decimal) {
            if (sort0 == Sort.DOUBLE || sort0 == Sort.DOUBLE_OBJ ||
                sort1 == Sort.DOUBLE || sort1 == Sort.DOUBLE_OBJ) {
                return primitive ? Definition.doubleType : Definition.doubleobjType;
            } else if (sort0 == Sort.FLOAT || sort0 == Sort.FLOAT_OBJ || sort1 == Sort.FLOAT || sort1 == Sort.FLOAT_OBJ) {
                return primitive ? Definition.floatType : Definition.floatobjType;
            }
        }

        if (sort0 == Sort.LONG || sort0 == Sort.LONG_OBJ ||
            sort1 == Sort.LONG || sort1 == Sort.LONG_OBJ) {
            return primitive ? Definition.longType : Definition.longobjType;
        } else if (sort0 == Sort.INT   || sort0 == Sort.INT_OBJ   ||
                   sort1 == Sort.INT   || sort1 == Sort.INT_OBJ   ||
                   sort0 == Sort.CHAR  || sort0 == Sort.CHAR_OBJ  ||
                   sort1 == Sort.CHAR  || sort1 == Sort.CHAR_OBJ  ||
                   sort0 == Sort.SHORT || sort0 == Sort.SHORT_OBJ ||
                   sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ ||
                   sort0 == Sort.BYTE  || sort0 == Sort.BYTE_OBJ  ||
                   sort1 == Sort.BYTE  || sort1 == Sort.BYTE_OBJ) {
            return primitive ? Definition.intType : Definition.intobjType;
        }

        return null;
    }

    public static Type promoteAdd(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.STRING || sort1 == Sort.STRING) {
            return Definition.stringType;
        }

        return promoteNumeric(from0, from1, true, true);
    }

    public static Type promoteXor(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0.bool || sort1.bool) {
            return Definition.booleanType;
        }

        return promoteNumeric(from0, from1, false, true);
    }

    public static Type promoteEquality(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return Definition.defType;
        }

        final boolean primitive = sort0.primitive && sort1.primitive;

        if (sort0.bool && sort1.bool) {
            return primitive ? Definition.booleanType : Definition.booleanobjType;
        }

        if (sort0.numeric && sort1.numeric) {
            return promoteNumeric(from0, from1, true, primitive);
        }

        return Definition.objectType;
    }

    public static Type promoteReference(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return Definition.defType;
        }

        if (sort0.primitive && sort1.primitive) {
            if (sort0.bool && sort1.bool) {
                return Definition.booleanType;
            }

            if (sort0.numeric && sort1.numeric) {
                return promoteNumeric(from0, from1, true, true);
            }
        }

        return Definition.objectType;
    }

    public static Type promoteConditional(final Type from0, final Type from1, final Object const0, final Object const1) {
        if (from0.equals(from1)) {
            return from0;
        }

        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return Definition.defType;
        }

        final boolean primitive = sort0.primitive && sort1.primitive;

        if (sort0.bool && sort1.bool) {
            return primitive ? Definition.booleanType : Definition.booleanobjType;
        }

        if (sort0.numeric && sort1.numeric) {
            if (sort0 == Sort.DOUBLE || sort0 == Sort.DOUBLE_OBJ || sort1 == Sort.DOUBLE || sort1 == Sort.DOUBLE_OBJ) {
                return primitive ? Definition.doubleType : Definition.doubleobjType;
            } else if (sort0 == Sort.FLOAT || sort0 == Sort.FLOAT_OBJ || sort1 == Sort.FLOAT || sort1 == Sort.FLOAT_OBJ) {
                return primitive ? Definition.floatType : Definition.floatobjType;
            } else if (sort0 == Sort.LONG || sort0 == Sort.LONG_OBJ || sort1 == Sort.LONG || sort1 == Sort.LONG_OBJ) {
                return sort0.primitive && sort1.primitive ? Definition.longType : Definition.longobjType;
            } else {
                if (sort0 == Sort.BYTE || sort0 == Sort.BYTE_OBJ) {
                    if (sort1 == Sort.BYTE || sort1 == Sort.BYTE_OBJ) {
                        return primitive ? Definition.byteType : Definition.byteobjType;
                    } else if (sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ) {
                        if (const1 != null) {
                            final short constant = (short)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return primitive ? Definition.byteType : Definition.byteobjType;
                            }
                        }

                        return primitive ? Definition.shortType : Definition.shortobjType;
                    } else if (sort1 == Sort.CHAR || sort1 == Sort.CHAR_OBJ) {
                        return primitive ? Definition.intType : Definition.intobjType;
                    } else if (sort1 == Sort.INT || sort1 == Sort.INT_OBJ) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return primitive ? Definition.byteType : Definition.byteobjType;
                            }
                        }

                        return primitive ? Definition.intType : Definition.intobjType;
                    }
                } else if (sort0 == Sort.SHORT || sort0 == Sort.SHORT_OBJ) {
                    if (sort1 == Sort.BYTE || sort1 == Sort.BYTE_OBJ) {
                        if (const0 != null) {
                            final short constant = (short)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return primitive ? Definition.byteType : Definition.byteobjType;
                            }
                        }

                        return primitive ? Definition.shortType : Definition.shortobjType;
                    } else if (sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ) {
                        return primitive ? Definition.shortType : Definition.shortobjType;
                    } else if (sort1 == Sort.CHAR || sort1 == Sort.CHAR_OBJ) {
                        return primitive ? Definition.intType : Definition.intobjType;
                    } else if (sort1 == Sort.INT || sort1 == Sort.INT_OBJ) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return primitive ? Definition.shortType : Definition.shortobjType;
                            }
                        }

                        return primitive ? Definition.intType : Definition.intobjType;
                    }
                } else if (sort0 == Sort.CHAR || sort0 == Sort.CHAR_OBJ) {
                    if (sort1 == Sort.BYTE || sort1 == Sort.BYTE_OBJ) {
                        return primitive ? Definition.intType : Definition.intobjType;
                    } else if (sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ) {
                        return primitive ? Definition.intType : Definition.intobjType;
                    } else if (sort1 == Sort.CHAR || sort1 == Sort.CHAR_OBJ) {
                        return primitive ? Definition.charType : Definition.charobjType;
                    } else if (sort1 == Sort.INT || sort1 == Sort.INT_OBJ) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return primitive ? Definition.byteType : Definition.byteobjType;
                            }
                        }

                        return primitive ? Definition.intType : Definition.intobjType;
                    }
                } else if (sort0 == Sort.INT || sort0 == Sort.INT_OBJ) {
                    if (sort1 == Sort.BYTE || sort1 == Sort.BYTE_OBJ) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return primitive ? Definition.byteType : Definition.byteobjType;
                            }
                        }

                        return primitive ? Definition.intType : Definition.intobjType;
                    } else if (sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return primitive ? Definition.byteType : Definition.byteobjType;
                            }
                        }

                        return primitive ? Definition.intType : Definition.intobjType;
                    } else if (sort1 == Sort.CHAR || sort1 == Sort.CHAR_OBJ) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return primitive ? Definition.byteType : Definition.byteobjType;
                            }
                        }

                        return primitive ? Definition.intType : Definition.intobjType;
                    } else if (sort1 == Sort.INT || sort1 == Sort.INT_OBJ) {
                        return primitive ? Definition.intType : Definition.intobjType;
                    }
                }
            }
        }

        // TODO: In the rare case we still haven't reached a correct promotion we need
        //       to calculate the highest upper bound for the two types and return that.
        //       However, for now we just return objectType that may require an extra cast.

        return Definition.objectType;
    }

    private AnalyzerCaster() {}
}
