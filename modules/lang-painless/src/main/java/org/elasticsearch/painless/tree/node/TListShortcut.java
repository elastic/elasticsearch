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

package org.elasticsearch.painless.tree.node;

import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.tree.node.Expression;
import org.elasticsearch.painless.tree.node.Target;
import org.elasticsearch.painless.tree.utility.Operation;
import org.objectweb.asm.commons.GeneratorAdapter;

public class TListShortcut extends Target {
    protected final Method getter;
    protected final Method setter;
    protected final Expression expression;

    public TListShortcut(final String location, final Method getter, final Method setter, final Expression expression) {
        super(location);

        this.getter = getter;
        this.setter = setter;
        this.expression = expression;
    }

    @Override
    protected void load(final GeneratorAdapter adapter) {

    }

    @Override
    protected void store(final GeneratorAdapter adapter, final Expression expression) {

    }

    @Override
    protected void pre(final GeneratorAdapter adapter, final Expression expression, final Operation operation) {

    }

    @Override
    protected void post(final GeneratorAdapter adapter, final Expression expression, final Operation operation) {

    }

    @Override
    protected void compound(final GeneratorAdapter adapter, final Expression expression) {

    }
}
