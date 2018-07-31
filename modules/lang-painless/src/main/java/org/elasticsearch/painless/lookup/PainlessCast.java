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

public class PainlessCast {

    /** Create a standard cast with no boxing/unboxing. */
    public static PainlessCast standard(Class<?> from, Class<?> to, boolean explicit) {
        return new PainlessCast(from, to, explicit, null, null, null, null);
    }

    /** Create a cast where the from type will be unboxed, and then the cast will be performed. */
    public static PainlessCast unboxFrom(Class<?> from, Class<?> to, boolean explicit, Class<?> unboxFrom) {
        return new PainlessCast(from, to, explicit, unboxFrom, null, null, null);
    }

    /** Create a cast where the to type will be unboxed, and then the cast will be performed. */
    public static PainlessCast unboxTo(Class<?> from, Class<?> to, boolean explicit, Class<?> unboxTo) {
        return new PainlessCast(from, to, explicit, null, unboxTo, null, null);
    }

    /** Create a cast where the from type will be boxed, and then the cast will be performed. */
    public static PainlessCast boxFrom(Class<?> from, Class<?> to, boolean explicit, Class<?> boxFrom) {
        return new PainlessCast(from, to, explicit, null, null, boxFrom, null);
    }

    /** Create a cast where the to type will be boxed, and then the cast will be performed. */
    public static PainlessCast boxTo(Class<?> from, Class<?> to, boolean explicit, Class<?> boxTo) {
        return new PainlessCast(from, to, explicit, null, null, null, boxTo);
    }

    public final Class<?> from;
    public final Class<?> to;
    public final boolean explicit;
    public final Class<?> unboxFrom;
    public final Class<?> unboxTo;
    public final Class<?> boxFrom;
    public final Class<?> boxTo;

    private PainlessCast(Class<?> from, Class<?> to, boolean explicit,
                         Class<?> unboxFrom, Class<?> unboxTo, Class<?> boxFrom, Class<?> boxTo) {
        this.from = from;
        this.to = to;
        this.explicit = explicit;
        this.unboxFrom = unboxFrom;
        this.unboxTo = unboxTo;
        this.boxFrom = boxFrom;
        this.boxTo = boxTo;
    }
}
