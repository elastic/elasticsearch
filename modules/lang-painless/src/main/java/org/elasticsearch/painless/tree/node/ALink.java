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

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.tree.analyzer.Operation;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

public abstract class ALink extends ANode {
    protected boolean load = true;
    protected boolean store = false;

    protected boolean statik = false;
    protected Type before = null;
    protected Type after = null;

    protected boolean statement = false;
    protected Object constant = null;

    protected AExpression expression = null;
    protected boolean pre = false;
    protected boolean post = false;
    protected Operation operation = null;
    protected boolean begincat = false;
    protected boolean endcat = false;
    protected Cast there = null;
    protected Cast back = null;

    public ALink(final String location) {
        super(location);
    }

    protected abstract ALink analyze(final CompilerSettings settings, final Definition definition, final Variables variables);
    protected abstract void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter);

    protected ALink copy(final ALink link) {
        load       = link.load;
        store      = link.store;

        statik     = link.statik;
        before     = link.before;
        after      = link.after;

        statement  = link.statement;
        constant   = link.constant;

        expression = link.expression;
        pre        = link.pre;
        post       = link.post;
        operation  = link.operation;
        begincat   = link.begincat;
        endcat     = link.endcat;
        there      = link.there;
        back       = link.back;

        return this;
    }
}
