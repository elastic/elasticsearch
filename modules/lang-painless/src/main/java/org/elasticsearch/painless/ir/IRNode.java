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

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.IRTreeVisitor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class IRNode {

    /* ---- begin decorations ---- */

    public interface IRDecoration {

    }

    private final Map<Class<? extends IRDecoration>, IRDecoration> decorations = new HashMap<>();

    @SuppressWarnings("unchecked")
    public <T extends IRDecoration> T attachDecoration(T decoration) {
        return (T)decorations.put(decoration.getClass(), decoration);
    }

    public <T extends IRDecoration> T removeDecoration(Class<T> type) {
        return type.cast(decorations.remove(type));
    }

    public <T extends IRDecoration> T getDecoration(Class<T> type) {
        return type.cast(decorations.get(type));
    }

    public boolean hasDecoration(Class<? extends IRDecoration> type) {
        return decorations.containsKey(type);
    }

    public <T extends IRDecoration> boolean copyDecorationFrom(IRNode copyFromIRNode, Class<T> type) {
        T decoration = copyFromIRNode.getDecoration(type);


        if (decoration != null) {
            attachDecoration(decoration);

            return true;
        }

        return false;
    }

    /* ---- end decorations, begin conditions ---- */

    public interface IRCondition {

    }

    private final Set<Class<? extends IRCondition>> conditions = new HashSet<>();

    public boolean attachCondition(Class<? extends IRCondition> type) {
        return conditions.add(type);
    }

    public boolean removeCondition(Class<? extends IRCondition> type) {
        return conditions.remove(type);
    }

    public boolean hasCondition(Class<? extends IRCondition> type) {
        return conditions.contains(type);
    }

    public boolean copyConditionFrom(IRNode copyFromIRNode, Class<? extends IRCondition> type) {
        if (copyFromIRNode.hasCondition(type)) {
            attachCondition(type);

            return true;
        }

        return false;
    }

    /* ---- end conditions, begin node data ---- */

    private final Location location;

    public Location getLocation() {
        return location;
    }

    /* ---- end node data, begin visitor ---- */

    public abstract <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope);
    public abstract <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope);

    /* ---- end visitor ---- */

    public IRNode(Location location) {
        this.location = location;
    }

}
