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
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;

/**
 * Used during the analysis phase to collect legal type casts and promotions
 * for type-checking and later to write necessary casts in the bytecode.
 */
public final class AnalyzerCaster {

    public static Cast getLegalCast(Location location, Type actual, Type expected, boolean explicit, boolean internal) {
        if (actual.equals(expected)) {
            return null;
        }

        switch (actual.sort) {
            case BOOL:
                switch (expected.sort) {
                    case DEF:
                        return new Cast(actual, Definition.DEF_TYPE, explicit, false, false, true, false);
                    case OBJECT:
                        if (Definition.OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case BOOL_OBJ:
                        if (internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);
                }

                break;
            case BYTE:
                switch (expected.sort) {
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return new Cast(actual, expected, explicit);
                    case CHAR:
                        if (explicit)
                            return new Cast(actual, expected, true);

                        break;
                    case DEF:
                        return new Cast(actual, Definition.DEF_TYPE, explicit, false, false, true, false);
                    case OBJECT:
                        if (Definition.OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case NUMBER:
                    case BYTE_OBJ:
                        if (internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case SHORT_OBJ:
                        if (internal)
                            return new Cast(actual,Definition.SHORT_TYPE, explicit, false, false, false, true);

                        break;
                    case INT_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.INT_TYPE, explicit, false, false, false, true);

                        break;
                    case LONG_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.LONG_TYPE, explicit, false, false, false, true);

                        break;
                    case FLOAT_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.FLOAT_TYPE, explicit, false, false, false, true);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.DOUBLE_TYPE, explicit, false, false, false, true);

                        break;
                    case CHAR_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.CHAR_TYPE, explicit, false, false, false, true);

                        break;
                }

                break;
            case SHORT:
                switch (expected.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return new Cast(actual, expected, explicit);
                    case BYTE:
                    case CHAR:
                        if (explicit)
                            return new Cast(actual, expected, true);

                        break;
                    case DEF:
                        return new Cast(actual, Definition.DEF_TYPE, explicit, false, false, true, false);
                    case OBJECT:
                        if (Definition.OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case NUMBER:
                    case SHORT_OBJ:
                        if (internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case INT_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.INT_TYPE, explicit, false, false, false, true);

                        break;
                    case LONG_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.LONG_TYPE, explicit, false, false, false, true);

                        break;
                    case FLOAT_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.FLOAT_TYPE, explicit, false, false, false, true);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.DOUBLE_TYPE, explicit, false, false, false, true);

                        break;
                    case BYTE_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.BYTE_TYPE, true, false, false, false, true);

                        break;
                    case CHAR_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.CHAR_TYPE, true, false, false, false, true);

                        break;
                }

                break;
            case CHAR:
                switch (expected.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return new Cast(actual, expected, explicit);
                    case BYTE:
                    case SHORT:
                        if (explicit)
                            return new Cast(actual, expected, true);

                        break;
                    case DEF:
                        return new Cast(actual, Definition.DEF_TYPE, explicit, false, false, true, false);
                    case OBJECT:
                        if (Definition.OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case NUMBER:
                    case CHAR_OBJ:
                        if (internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case STRING:
                        return new Cast(actual, Definition.STRING_TYPE, explicit, false, false, false, false);
                    case INT_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.INT_TYPE, explicit, false, false, false, true);

                        break;
                    case LONG_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.LONG_TYPE, explicit, false, false, false, true);

                        break;
                    case FLOAT_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.FLOAT_TYPE, explicit, false, false, false, true);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.DOUBLE_TYPE, explicit, false, false, false, true);

                        break;
                    case BYTE_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.BYTE_TYPE, true, false, false, false, true);

                        break;
                    case SHORT_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.SHORT_TYPE, true, false, false, false, true);

                        break;
                }

                break;
            case INT:
                switch (expected.sort) {
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return new Cast(actual, expected, explicit);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                        if (explicit)
                            return new Cast(actual, expected, true);

                        break;
                    case DEF:
                        return new Cast(actual, Definition.DEF_TYPE, explicit, false, false, true, false);
                    case OBJECT:
                        if (Definition.OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case NUMBER:
                    case INT_OBJ:
                        if (internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case LONG_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.LONG_TYPE, explicit, false, false, false, true);

                        break;
                    case FLOAT_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.FLOAT_TYPE, explicit, false, false, false, true);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.DOUBLE_TYPE, explicit, false, false, false, true);

                        break;
                    case BYTE_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.BYTE_TYPE, true, false, false, false, true);

                        break;
                    case SHORT_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.SHORT_TYPE, true, false, false, false, true);

                        break;
                    case CHAR_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.CHAR_TYPE, true, false, false, false, true);

                        break;
                }

                break;
            case LONG:
                switch (expected.sort) {
                    case FLOAT:
                    case DOUBLE:
                        return new Cast(actual, expected, explicit);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                        if (explicit)
                            return new Cast(actual, expected, true);

                        break;
                    case DEF:
                        return new Cast(actual, Definition.DEF_TYPE, explicit, false, false, true, false);
                    case OBJECT:
                        if (Definition.OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case NUMBER:
                    case LONG_OBJ:
                        if (internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case FLOAT_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.FLOAT_TYPE, explicit, false, false, false, true);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.DOUBLE_TYPE, explicit, false, false, false, true);

                        break;
                    case BYTE_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.BYTE_TYPE, true, false, false, false, true);

                        break;
                    case SHORT_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.SHORT_TYPE, true, false, false, false, true);

                        break;
                    case CHAR_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.CHAR_TYPE, true, false, false, false, true);

                        break;
                    case INT_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.INT_TYPE, true, false, false, false, true);

                        break;
                }

                break;
            case FLOAT:
                switch (expected.sort) {
                    case DOUBLE:
                        return new Cast(actual, expected, explicit);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case FLOAT:
                        if (explicit)
                            return new Cast(actual, expected, true);

                        break;
                    case DEF:
                        return new Cast(actual, Definition.DEF_TYPE, explicit, false, false, true, false);
                    case OBJECT:
                        if (Definition.OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case NUMBER:
                    case FLOAT_OBJ:
                        if (internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(actual, Definition.DOUBLE_TYPE, explicit, false, false, false, true);

                        break;
                    case BYTE_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.BYTE_TYPE, true, false, false, false, true);

                        break;
                    case SHORT_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.SHORT_TYPE, true, false, false, false, true);

                        break;
                    case CHAR_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.CHAR_TYPE, true, false, false, false, true);

                        break;
                    case INT_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.INT_TYPE, true, false, false, false, true);

                        break;
                    case LONG_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.LONG_TYPE, true, false, false, false, true);

                        break;
                }

                break;
            case DOUBLE:
                switch (expected.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case FLOAT:
                        if (explicit)
                            return new Cast(actual, expected, true);

                        break;
                    case DEF:
                        return new Cast(actual, Definition.DEF_TYPE, explicit, false, false, true, false);
                    case OBJECT:
                        if (Definition.OBJECT_TYPE.equals(expected) && internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case NUMBER:
                    case DOUBLE_OBJ:
                        if (internal)
                            return new Cast(actual, actual, explicit, false, false, false, true);

                        break;
                    case BYTE_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.BYTE_TYPE, true, false, false, false, true);

                        break;
                    case SHORT_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.SHORT_TYPE, true, false, false, false, true);

                        break;
                    case CHAR_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.CHAR_TYPE, true, false, false, false, true);

                        break;
                    case INT_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.INT_TYPE, true, false, false, false, true);

                        break;
                    case LONG_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.LONG_TYPE, true, false, false, false, true);

                        break;
                    case FLOAT_OBJ:
                        if (explicit && internal)
                            return new Cast(actual, Definition.FLOAT_TYPE, true, false, false, false, true);

                        break;
                }

                break;
            case OBJECT:
                if (Definition.OBJECT_TYPE.equals(actual))
                    switch (expected.sort) {
                        case BYTE:
                            if (internal && explicit)
                                return new Cast(actual, Definition.BYTE_OBJ_TYPE, true, false, true, false, false);

                            break;
                        case SHORT:
                            if (internal && explicit)
                                return new Cast(actual, Definition.SHORT_OBJ_TYPE, true, false, true, false, false);

                            break;
                        case CHAR:
                            if (internal && explicit)
                                return new Cast(actual, Definition.CHAR_OBJ_TYPE, true, false, true, false, false);

                            break;
                        case INT:
                            if (internal && explicit)
                                return new Cast(actual, Definition.INT_OBJ_TYPE, true, false, true, false, false);

                            break;
                        case LONG:
                            if (internal && explicit)
                                return new Cast(actual, Definition.LONG_OBJ_TYPE, true, false, true, false, false);

                            break;
                        case FLOAT:
                            if (internal && explicit)
                                return new Cast(actual, Definition.FLOAT_OBJ_TYPE, true, false, true, false, false);

                            break;
                        case DOUBLE:
                            if (internal && explicit)
                                return new Cast(actual, Definition.DOUBLE_OBJ_TYPE, true, false, true, false, false);

                            break;
                    }
                break;
            case NUMBER:
                switch (expected.sort) {
                    case BYTE:
                        if (internal && explicit)
                            return new Cast(actual, Definition.BYTE_OBJ_TYPE, true, false, true, false, false);

                        break;
                    case SHORT:
                        if (internal && explicit)
                            return new Cast(actual, Definition.SHORT_OBJ_TYPE, true, false, true, false, false);

                        break;
                    case CHAR:
                        if (internal && explicit)
                            return new Cast(actual, Definition.CHAR_OBJ_TYPE, true, false, true, false, false);

                        break;
                    case INT:
                        if (internal && explicit)
                            return new Cast(actual, Definition.INT_OBJ_TYPE, true, false, true, false, false);

                        break;
                    case LONG:
                        if (internal && explicit)
                            return new Cast(actual, Definition.LONG_OBJ_TYPE, true, false, true, false, false);

                        break;
                    case FLOAT:
                        if (internal && explicit)
                            return new Cast(actual, Definition.FLOAT_OBJ_TYPE, true, false, true, false, false);

                        break;
                    case DOUBLE:
                        if (internal && explicit)
                            return new Cast(actual, Definition.DOUBLE_OBJ_TYPE, true, false, true, false, false);

                        break;
                }

                break;
            case BOOL_OBJ:
                switch (expected.sort) {
                    case BOOL:
                        if (internal)
                            return new Cast(actual, expected, explicit, true, false, false, false);

                        break;
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
                        if (internal)
                            return new Cast(actual, expected, explicit, true, false, false, false);

                        break;
                    case CHAR:
                        if (internal && explicit)
                            return new Cast(actual, expected, true, true, false, false, false);

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
                        if (internal)
                            return new Cast(actual, expected, explicit, true, false, false, false);

                        break;
                    case BYTE:
                    case CHAR:
                        if (internal && explicit)
                            return new Cast(actual, expected, true, true, false, false, false);

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
                        if (internal)
                            return new Cast(actual, expected, explicit, true, false, false, false);

                        break;
                    case BYTE:
                    case SHORT:
                        if (internal && explicit)
                            return new Cast(actual, expected, true, true, false, false, false);

                        break;
                }

                break;
            case INT_OBJ:
                switch (expected.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (internal)
                            return new Cast(actual, expected, explicit, true, false, false, false);

                        break;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                        if (internal && explicit)
                            return new Cast(actual, expected, true, true, false, false, false);

                        break;
                }

                break;
            case LONG_OBJ:
                switch (expected.sort) {
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (internal)
                            return new Cast(actual, expected, explicit, true, false, false, false);

                        break;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                        if (internal && explicit)
                            return new Cast(actual, expected, true, true, false, false, false);

                        break;
                }

                break;
            case FLOAT_OBJ:
                switch (expected.sort) {
                    case FLOAT:
                    case DOUBLE:
                        if (internal)
                            return new Cast(actual, expected, explicit, true, false, false, false);

                        break;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                        if (internal && explicit)
                            return new Cast(actual, expected, true, true, false, false, false);

                        break;
                }

                break;
            case DOUBLE_OBJ:
                switch (expected.sort) {
                    case FLOAT:
                    case DOUBLE:
                        if (internal)
                            return new Cast(actual, expected, explicit, true, false, false, false);

                        break;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                        if (internal && explicit)
                            return new Cast(actual, expected, true, true, false, false, false);

                        break;
                }

                break;
            case DEF:
                switch (expected.sort) {
                    case BOOL:
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                            return new Cast(actual, expected, explicit, true, false, false, false);
                }

                break;
            case STRING:
                switch (expected.sort) {
                    case CHAR:
                        if (explicit)
                            return new Cast(actual, expected, true, false, false, false, false);

                        break;
                }

                break;
        }

        if (actual.sort == Sort.DEF || expected.sort == Sort.DEF ||
            expected.clazz.isAssignableFrom(actual.clazz) ||
            explicit && actual.clazz.isAssignableFrom(expected.clazz)) {
            return new Cast(actual, expected, explicit);
        } else {
            throw location.createError(new ClassCastException("Cannot cast from [" + actual.name + "] to [" + expected.name + "]."));
        }
    }

    public static Object constCast(Location location, final Object constant, final Cast cast) {
        final Sort fsort = cast.from.sort;
        final Sort tsort = cast.to.sort;

        if (fsort == tsort) {
            return constant;
        } else if (fsort == Sort.STRING && tsort == Sort.CHAR) {
            return Utility.StringTochar((String)constant);
        } else if (fsort == Sort.CHAR && tsort == Sort.STRING) {
            return Utility.charToString((char)constant);
        } else if (fsort.numeric && tsort.numeric) {
            final Number number;

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
                    throw location.createError(new IllegalStateException("Cannot cast from " +
                        "[" + cast.from.clazz.getCanonicalName() + "] to [" + cast.to.clazz.getCanonicalName() + "]."));
            }
        } else {
            throw location.createError(new IllegalStateException("Cannot cast from " +
                "[" + cast.from.clazz.getCanonicalName() + "] to [" + cast.to.clazz.getCanonicalName() + "]."));
        }
    }

    public static Type promoteNumeric(Type from, boolean decimal) {
        final Sort sort = from.sort;

        if (sort == Sort.DEF) {
            return Definition.DEF_TYPE;
        } else if ((sort == Sort.DOUBLE) && decimal) {
            return Definition.DOUBLE_TYPE;
        } else if ((sort == Sort.FLOAT) && decimal) {
            return  Definition.FLOAT_TYPE;
        } else if (sort == Sort.LONG) {
            return Definition.LONG_TYPE;
        } else if (sort == Sort.INT || sort == Sort.CHAR || sort == Sort.SHORT || sort == Sort.BYTE) {
            return Definition.INT_TYPE;
        }

        return null;
    }

    public static Type promoteNumeric(Type from0, Type from1, boolean decimal) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return Definition.DEF_TYPE;
        }

        if (decimal) {
            if (sort0 == Sort.DOUBLE || sort1 == Sort.DOUBLE) {
                return Definition.DOUBLE_TYPE;
            } else if (sort0 == Sort.FLOAT || sort1 == Sort.FLOAT) {
                return Definition.FLOAT_TYPE;
            }
        }

        if (sort0 == Sort.LONG || sort1 == Sort.LONG) {
            return Definition.LONG_TYPE;
        } else if (sort0 == Sort.INT   || sort1 == Sort.INT   ||
                   sort0 == Sort.CHAR  || sort1 == Sort.CHAR  ||
                   sort0 == Sort.SHORT || sort1 == Sort.SHORT ||
                   sort0 == Sort.BYTE  || sort1 == Sort.BYTE) {
            return Definition.INT_TYPE;
        }

        return null;
    }

    public static Type promoteAdd(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.STRING || sort1 == Sort.STRING) {
            return Definition.STRING_TYPE;
        }

        return promoteNumeric(from0, from1, true);
    }

    public static Type promoteXor(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return Definition.DEF_TYPE;
        }

        if (sort0.bool || sort1.bool) {
            return Definition.BOOLEAN_TYPE;
        }

        return promoteNumeric(from0, from1, false);
    }

    public static Type promoteEquality(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return Definition.DEF_TYPE;
        }

        if (sort0.primitive && sort1.primitive) {
            if (sort0.bool && sort1.bool) {
                return Definition.BOOLEAN_TYPE;
            }

            if (sort0.numeric && sort1.numeric) {
                return promoteNumeric(from0, from1, true);
            }
        }

        return Definition.OBJECT_TYPE;
    }

    public static Type promoteConditional(final Type from0, final Type from1, final Object const0, final Object const1) {
        if (from0.equals(from1)) {
            return from0;
        }

        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return Definition.DEF_TYPE;
        }

        if (sort0.primitive && sort1.primitive) {
            if (sort0.bool && sort1.bool) {
                return Definition.BOOLEAN_TYPE;
            }

            if (sort0 == Sort.DOUBLE || sort1 == Sort.DOUBLE) {
                return Definition.DOUBLE_TYPE;
            } else if (sort0 == Sort.FLOAT || sort1 == Sort.FLOAT) {
                return Definition.FLOAT_TYPE;
            } else if (sort0 == Sort.LONG || sort1 == Sort.LONG) {
                return Definition.LONG_TYPE;
            } else {
                if (sort0 == Sort.BYTE) {
                    if (sort1 == Sort.BYTE) {
                        return Definition.BYTE_TYPE;
                    } else if (sort1 == Sort.SHORT) {
                        if (const1 != null) {
                            final short constant = (short)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return Definition.BYTE_TYPE;
                            }
                        }

                        return Definition.SHORT_TYPE;
                    } else if (sort1 == Sort.CHAR) {
                        return Definition.INT_TYPE;
                    } else if (sort1 == Sort.INT) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return Definition.BYTE_TYPE;
                            }
                        }

                        return Definition.INT_TYPE;
                    }
                } else if (sort0 == Sort.SHORT) {
                    if (sort1 == Sort.BYTE) {
                        if (const0 != null) {
                            final short constant = (short)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return Definition.BYTE_TYPE;
                            }
                        }

                        return Definition.SHORT_TYPE;
                    } else if (sort1 == Sort.SHORT) {
                        return Definition.SHORT_TYPE;
                    } else if (sort1 == Sort.CHAR) {
                        return Definition.INT_TYPE;
                    } else if (sort1 == Sort.INT) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return Definition.SHORT_TYPE;
                            }
                        }

                        return Definition.INT_TYPE;
                    }
                } else if (sort0 == Sort.CHAR) {
                    if (sort1 == Sort.BYTE) {
                        return Definition.INT_TYPE;
                    } else if (sort1 == Sort.SHORT) {
                        return Definition.INT_TYPE;
                    } else if (sort1 == Sort.CHAR) {
                        return Definition.CHAR_TYPE;
                    } else if (sort1 == Sort.INT) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return Definition.BYTE_TYPE;
                            }
                        }

                        return Definition.INT_TYPE;
                    }
                } else if (sort0 == Sort.INT) {
                    if (sort1 == Sort.BYTE) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return Definition.BYTE_TYPE;
                            }
                        }

                        return Definition.INT_TYPE;
                    } else if (sort1 == Sort.SHORT) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return Definition.BYTE_TYPE;
                            }
                        }

                        return Definition.INT_TYPE;
                    } else if (sort1 == Sort.CHAR) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return Definition.BYTE_TYPE;
                            }
                        }

                        return Definition.INT_TYPE;
                    } else if (sort1 == Sort.INT) {
                        return Definition.INT_TYPE;
                    }
                }
            }
        }

        // TODO: In the rare case we still haven't reached a correct promotion we need
        //       to calculate the highest upper bound for the two types and return that.
        //       However, for now we just return objectType that may require an extra cast.

        return Definition.OBJECT_TYPE;
    }

    private AnalyzerCaster() {}
}
