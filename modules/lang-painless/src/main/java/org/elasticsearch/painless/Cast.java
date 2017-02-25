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

import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;

import java.util.function.Function;

/**
 * Casting strategies. Many, but not all, casting strategies support a "next" strategy to allow building compound strategies like "unbox and
 * then convert from char to int". These are always read from "outside inwards", meaning that a strategy is first executed and then the
 * "next" strategy is executed.
 */
public abstract class Cast { // NOCOMMIT rename
    Cast() {} // NOCOMMIT make private

    public abstract boolean castRequired();
    public abstract void write(MethodWriter writer);
    public abstract Object castConstant(Location location, Object constant);
    public abstract String toString();

    /**
     * Cast that doesn't do anything. Used when you don't need to cast at all.
     */
    static final Cast NOOP = new Cast() {
        @Override
        public void write(MethodWriter writer) {
        }

        @Override
        public boolean castRequired() {
            return false;
        }

        @Override
        public Object castConstant(Location location, Object constant) {
            return constant;
        }

        @Override
        public String toString() {
            return "noop";
        }
    };

    /**
     * Promote or demote a numeric.
     */
    public static class Numeric extends Cast {
        private final Type from;
        private final Type to;
        private final Cast next;

        public Numeric(Type from, Type to, Cast next) {
            if (from.equals(to)) {
                throw new IllegalArgumentException("From and to must not be equal but were [" + from + "].");
            }
            if (to.clazz.isAssignableFrom(from.clazz)) {
                throw new IllegalArgumentException("Promote isn't needed for to [" + to + "] is assignable to from [" + from + "]");
            }
            if (false == from.sort.numeric && from.sort.primitive) {
                throw new IllegalArgumentException("From [" + from + "] must be primitive and numeric.");
            }
            if (false == to.sort.numeric && to.sort.primitive) {
                throw new IllegalArgumentException("To [" + to + "] must be primitive and numeric.");
            }
            if (next == null) {
                throw new IllegalArgumentException("Next must not be null.");
            }
            this.from = from;
            this.to = to;
            this.next = next;
        }

        public Numeric(Type from, Type to) {
            this(from, to, Cast.NOOP); // NOCOMMIT use this more in analyzerCaster
        }

        @Override
        public boolean castRequired() {
            return true;
        }

        @Override
        public void write(MethodWriter writer) {
            writer.cast(from.type, to.type);
            next.write(writer);
        }

        @Override
        public Object castConstant(Location location, Object constant) {
            return next.castConstant(location, internalCastConstant(location, constant));
        }

        private Object internalCastConstant(Location location, Object constant) {
            Number number = from.sort == Sort.CHAR ? (int)(char)constant : (Number)constant;
            switch (to.sort) {
            case BYTE:   return number.byteValue();
            case SHORT:  return number.shortValue();
            case CHAR:   return (char)number.intValue();
            case INT:    return number.intValue();
            case LONG:   return number.longValue();
            case FLOAT:  return number.floatValue();
            case DOUBLE: return number.doubleValue();
            default:
                throw location.createError(new IllegalStateException("Cannot cast from " +
                    "[" + from.clazz.getCanonicalName() + "] to [" + to.clazz.getCanonicalName() + "]."));
            }
        }

        @Override
        public String toString() {
            if (next == Cast.NOOP) {
                return "(Numeric " + from + " " + to + ")";
            } else {
                return "(Numeric " + from + " " + to + " " + next + ")";
            }
        }
    }

    /**
     * Box some boxable type.
     */
    public static class Box extends Cast {
        private final Type from;

        public Box(Type from) {
            if (from.sort.boxed == null) {
                throw new IllegalArgumentException("From must be a boxable type but was [" + from + "]");
            }
            this.from = from;
        }

        @Override
        public boolean castRequired() {
            return true;
        }

        @Override
        public void write(MethodWriter writer) {
            writer.box(from.type);
        }

        @Override
        public Object castConstant(Location location, Object constant) {
            throw new UnsupportedOperationException("Boxed values can't be written as constants");
        }

        @Override
        public String toString() {
            return "(Box " + from + ")";
        }
    }

    /**
     * Unbox some boxable type.
     */
    public static class Unbox extends Cast {
        private final Type to;
        private final Cast next;

        public Unbox(Type to, Cast next) {
            if (to.sort.boxed == null) {
                throw new IllegalArgumentException("To must be a boxable type but was [" + to + "]");
            }
            if (next == null) {
                throw new IllegalArgumentException("Next must not be null");
            }
            this.to = to;
            this.next = next;
        }

        public Unbox(Type to) {
            this(to, Cast.NOOP);
        }

        @Override
        public boolean castRequired() {
            return true;
        }

        @Override
        public void write(MethodWriter writer) {
            writer.unbox(to.type);
            next.write(writer);
        }

        @Override
        public Object castConstant(Location location, Object constant) {
            // Constants are always boxed inside the compiler. We unbox them when writing instead.
            return next.castConstant(location, constant);
        }

        @Override
        public String toString() {
            if (next == Cast.NOOP) {
                return "(Unbox " + to + ")";
            } else {
                return "(Unbox " + to + " " + next + ")";
            }
        }
    }

    /**
     * Performs a checked cast to narrow from a wider type to a more specific one. For example
     * {@code Number n = Integer.valueOf(5); Integer i = (Integer) n}.
     */
    public static class CheckedCast extends Cast {
        private final Type to;

        public CheckedCast(Type to) {
            this.to = to;
        }

        @Override
        public boolean castRequired() {
            return true;
        }

        @Override
        public void write(MethodWriter writer) {
            writer.checkCast(to.type);
        }

        @Override
        public Object castConstant(Location location, Object constant) {
            return to.clazz.cast(constant);
        }

        @Override
        public String toString() {
            return "(CheckedCast " + to + ")";
        }
    }

    /**
     * Invoke a static method to do the cast. Used for {@code char c = 'c'}
     */
    public static class InvokeStatic extends Cast {
        private final org.objectweb.asm.Type owner;
        private final org.objectweb.asm.commons.Method method;
        private final Function<Object, Object> castConstant;

        public InvokeStatic(org.objectweb.asm.Type owner, org.objectweb.asm.commons.Method method, Function<Object, Object> castConstant) {
            this.owner = owner;
            this.method = method;
            this.castConstant = castConstant;
        }

        @Override
        public boolean castRequired() {
            return true;
        }

        @Override
        public void write(MethodWriter writer) {
            writer.invokeStatic(owner, method);
        }

        @Override
        public Object castConstant(Location location, Object constant) {
            try {
                return castConstant.apply(constant);
            } catch (Throwable e) {
                throw location.createError(new IllegalArgumentException("Failed to cast constant: " + e.getMessage(), e));
            }
        }

        @Override
        public String toString() {
            return "(InvokeStatic " + owner.getClassName() + "#" + method.getName() + ")";
        }
    }
}
