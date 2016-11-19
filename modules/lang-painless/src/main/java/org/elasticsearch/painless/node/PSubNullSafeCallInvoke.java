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

package org.elasticsearch.painless.node;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Label;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Implements a call who's value is null if the prefix is null rather than throwing an NPE.
 */
public class PSubNullSafeCallInvoke extends AExpression {
    /**
     * The expression guarded by the null check.
     */
    private final AExpression guarded;
    /**
     * The {@link EElvis} operator containing this node to which we can emit a jump to put the replacement value for null on the stack.
     * Null if there isn't such an operator.
     */
    private final EElvis defaultForNull;

    public PSubNullSafeCallInvoke(Location location, AExpression guarded, @Nullable EElvis defaultForNull) {
        super(location);
        this.guarded = requireNonNull(guarded);
        this.defaultForNull = defaultForNull;
    }

    @Override
    void extractVariables(Set<String> variables) {
        guarded.extractVariables(variables);
    }

    @Override
    void analyze(Locals locals) {
        guarded.analyze(locals);
        actual = guarded.actual;
        if (actual.sort.primitive && defaultForNull == null) {
            throw new IllegalArgumentException("Result of null safe dereference must be nullable unless followed by ?:");
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        Label nullLabel = defaultForNull == null ? new Label() : defaultForNull.nullLabel();
        writer.dup();
        writer.ifNull(nullLabel);
        guarded.write(writer, globals);
        if (defaultForNull == null) {
            writer.mark(nullLabel);
        }
    }
}
