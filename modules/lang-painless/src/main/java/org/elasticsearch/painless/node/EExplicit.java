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

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents an explicit cast.
 */
public final class EExplicit extends AExpression {

    final String type;
    AExpression child;

    public EExplicit(final int line, final String location, final String type, final AExpression child) {
        super(line, location);

        this.type = type;
        this.child = child;
    }

    @Override
    void analyze(final CompilerSettings settings, final Variables variables) {
        try {
            actual = Definition.getType(this.type);
        } catch (final IllegalArgumentException exception) {
            throw new IllegalArgumentException(error("Not a type [" + this.type + "]."));
        }

        child.expected = actual;
        child.explicit = true;
        child.analyze(settings, variables);
        child = child.cast(settings, variables);
    }

    @Override
    void write(final CompilerSettings settings, final MethodWriter adapter) {
        throw new IllegalArgumentException(error("Illegal tree structure."));
    }

    AExpression cast(final CompilerSettings settings, final Variables variables) {
        child.expected = expected;
        child.explicit = explicit;

        return child.cast(settings, variables);
    }
}
