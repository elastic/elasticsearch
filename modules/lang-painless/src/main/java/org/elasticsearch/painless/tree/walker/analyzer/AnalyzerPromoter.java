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

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Pair;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;

class AnalyzerPromoter {
    private final Definition definition;

    AnalyzerPromoter(final Definition definition) {
        this.definition = definition;
    }

    Type promoteNumeric(final Type from, final boolean decimal, final boolean primitive) {
        final Sort sort = from.sort;

        if (sort == Sort.DEF) {
            return definition.defType;
        } else if ((sort == Sort.DOUBLE || sort == Sort.DOUBLE_OBJ || sort == Sort.NUMBER) && decimal) {
            return primitive ? definition.doubleType : definition.doubleobjType;
        } else if ((sort == Sort.FLOAT || sort == Sort.FLOAT_OBJ) && decimal) {
            return primitive ? definition.floatType : definition.floatobjType;
        } else if (sort == Sort.LONG || sort == Sort.LONG_OBJ || sort == Sort.NUMBER) {
            return primitive ? definition.longType : definition.longobjType;
        } else if (sort.numeric) {
            return primitive ? definition.intType : definition.intobjType;
        }

        return null;
    }

    Type promoteNumeric(final Type from0, final Type from1, final boolean decimal, final boolean primitive) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return definition.defType;
        }

        if (decimal) {
            if (sort0 == Sort.DOUBLE || sort0 == Sort.DOUBLE_OBJ || sort0 == Sort.NUMBER ||
                sort1 == Sort.DOUBLE || sort1 == Sort.DOUBLE_OBJ || sort1 == Sort.NUMBER) {
                return primitive ? definition.doubleType : definition.doubleobjType;
            } else if (sort0 == Sort.FLOAT || sort0 == Sort.FLOAT_OBJ || sort1 == Sort.FLOAT || sort1 == Sort.FLOAT_OBJ) {
                return primitive ? definition.floatType : definition.floatobjType;
            }
        }

        if (sort0 == Sort.LONG || sort0 == Sort.LONG_OBJ || sort0 == Sort.NUMBER ||
            sort1 == Sort.LONG || sort1 == Sort.LONG_OBJ || sort1 == Sort.NUMBER) {
            return primitive ? definition.longType : definition.longobjType;
        } else if (sort0.numeric && sort1.numeric) {
            return primitive ? definition.intType : definition.intobjType;
        }

        return null;
    }

    Type promoteAdd(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.STRING || sort1 == Sort.STRING) {
            return definition.stringType;
        }

        return promoteNumeric(from0, from1, true, true);
    }

    Type promoteXor(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0.bool || sort1.bool) {
            return definition.booleanType;
        }

        return promoteNumeric(from0, from1, false, true);
    }

    Type promoteEquality(final Type from0, final Type from1) {
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
            return promoteNumeric(from0, from1, true, primitive);
        }

        return definition.objectType;
    }

    Type promoteReference(final Type from0, final Type from1) {
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
                return promoteNumeric(from0, from1, true, true);
            }
        }

        return definition.objectType;
    }

    Type promoteConditional(final Type from0, final Type from1, final Object const0, final Object const1) {
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
