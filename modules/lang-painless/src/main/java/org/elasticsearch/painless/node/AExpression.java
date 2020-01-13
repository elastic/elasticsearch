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

import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * The superclass for all E* (expression) and P* (postfix) nodes.
 */
public abstract class AExpression extends ANode {

    public static class Input {

        /**
         * Set to false when an expression will not be read from such as
         * a basic assignment.  Note this variable is always set by the parent
         * as input.
         */
        boolean read = true;

        /**
         * Set to the expected type this node needs to be.  Note this variable
         * is always set by the parent as input and should never be read from.
         */
        Class<?> expected = null;

        /**
         * Set by {@link EExplicit} if a cast made on an expression node should be
         * explicit.
         */
        boolean explicit = false;

        /**
         * Set to true if a cast is allowed to boxed/unboxed.  This is used
         * for method arguments because casting may be required.
         */
        boolean internal = false;
    }

    public static class Output {

        /**
         * Set to true when an expression can be considered a stand alone
         * statement.  Used to prevent extraneous bytecode. This is always
         * set by the node as output.
         */
        boolean statement = false;

        /**
         * Set to the actual type this node is.  Note this variable is always
         * set by the node as output and should only be read from outside of the
         * node itself.  <b>Also, actual can always be read after a cast is
         * called on this node to get the type of the node after the cast.</b>
         */
        Class<?> actual = null;
    }

    /**
     * Prefix is the predecessor to this node in a variable chain.
     * This is used to analyze and write variable chains in a
     * more natural order since the parent node of a variable
     * chain will want the data from the final postfix to be
     * analyzed.
     */
    AExpression prefix;

    // TODO: remove placeholders once analysis and write are combined into build
    Input input = null;
    Output output = null;
    PainlessCast cast = null;

    /**
     * Standard constructor with location used for error tracking.
     */
    AExpression(Location location) {
        super(location);

        prefix = null;
    }

    /**
     * This constructor is used by variable/method chains when postfixes are specified.
     */
    AExpression(Location location, AExpression prefix) {
        super(location);

        this.prefix = Objects.requireNonNull(prefix);
    }

    /**
     * Checks for errors and collects data for the writing phase.
     */
    Output analyze(ScriptRoot scriptRoot, Scope scope, Input input) {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes ASM based on the data collected during the analysis phase.
     */
    abstract ExpressionNode write(ClassNode classNode);

    void cast() {
        cast = AnalyzerCaster.getLegalCast(location, output.actual, input.expected, input.explicit, input.internal);
    }

    ExpressionNode cast(ExpressionNode expressionNode) {
        if (cast == null) {
            return expressionNode;
        }

        CastNode castNode = new CastNode();
        castNode.setLocation(location);
        castNode.setExpressionType(cast.targetType);
        castNode.setCast(cast);
        castNode.setChildNode(expressionNode);

        return castNode;
    }
}
