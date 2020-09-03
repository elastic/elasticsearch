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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.WriteScope;

public abstract class IRNode {

    /* ---- begin node data ---- */

    protected Location location;

    public void setLocation(Location location) {
        this.location = location;
    }

    public Location getLocation() {
        return location;
    }

    /* ---- end node data, begin visitor ---- */

    /**
     * Callback to visit an ir tree node.
     */
    public abstract <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope);

    /**
     * Visits all child ir tree nodes for this ir tree node.
     */
    public abstract <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope);

    /* ---- end visitor ---- */

    protected abstract void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope);
}
