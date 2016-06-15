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

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Locals;
import org.objectweb.asm.Label;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a boolean expression.
 */
public final class EBool extends AExpression {

    final Operation operation;
    AExpression left;
    AExpression right;

    public EBool(Location location, Operation operation, AExpression left, AExpression right) {
        super(location);

        this.operation = operation;
        this.left = left;
        this.right = right;
    }

    @Override
    void analyze(Locals locals) {
        left.expected = Definition.BOOLEAN_TYPE;
        left.analyze(locals);
        left = left.cast(locals);

        right.expected = Definition.BOOLEAN_TYPE;
        right.analyze(locals);
        right = right.cast(locals);

        if (left.constant != null && right.constant != null) {
            if (operation == Operation.AND) {
                constant = (boolean)left.constant && (boolean)right.constant;
            } else if (operation == Operation.OR) {
                constant = (boolean)left.constant || (boolean)right.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = Definition.BOOLEAN_TYPE;
    }

    @Override
    void write(MethodWriter writer) {
        if (tru != null || fals != null) {
            if (operation == Operation.AND) {
                Label localfals = fals == null ? new Label() : fals;

                left.fals = localfals;
                right.tru = tru;
                right.fals = fals;

                left.write(writer);
                right.write(writer);

                if (fals == null) {
                    writer.mark(localfals);
                }
            } else if (operation == Operation.OR) {
                Label localtru = tru == null ? new Label() : tru;

                left.tru = localtru;
                right.tru = tru;
                right.fals = fals;

                left.write(writer);
                right.write(writer);

                if (tru == null) {
                    writer.mark(localtru);
                }
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        } else {
            if (operation == Operation.AND) {
                Label localfals = new Label();
                Label end = new Label();

                left.fals = localfals;
                right.fals = localfals;

                left.write(writer);
                right.write(writer);

                writer.push(true);
                writer.goTo(end);
                writer.mark(localfals);
                writer.push(false);
                writer.mark(end);
            } else if (operation == Operation.OR) {
                Label localtru = new Label();
                Label localfals = new Label();
                Label end = new Label();

                left.tru = localtru;
                right.fals = localfals;

                left.write(writer);
                right.write(writer);

                writer.mark(localtru);
                writer.push(true);
                writer.goTo(end);
                writer.mark(localfals);
                writer.push(false);
                writer.mark(end);
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }
    }
}
