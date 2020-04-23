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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.DefaultSemanticAnalysisPhase;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.StandardConstant;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Represents a regex constant. All regexes are constants.
 */
public class ERegex extends AExpression {

    private final String pattern;
    private final String flags;

    public ERegex(int identifier, Location location, String pattern, String flags) {
        super(identifier, location);

        this.pattern = Objects.requireNonNull(pattern);
        this.flags = Objects.requireNonNull(flags);
    }

    public String getPattern() {
        return pattern;
    }

    public String getFlags() {
        return flags;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitRegex(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, ERegex userRegexNode, SemanticScope semanticScope) {

        String pattern = userRegexNode.getPattern();
        String flags = userRegexNode.getFlags();

        if (semanticScope.getCondition(userRegexNode, Write.class)) {
            throw userRegexNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to regex constant [" + pattern + "] with flags [" + flags + "]"));
        }

        if (semanticScope.getCondition(userRegexNode, Read.class) == false) {
            throw userRegexNode.createError(new IllegalArgumentException(
                    "not a statement: regex constant [" + pattern + "] with flags [" + flags + "] not used"));
        }

        if (semanticScope.getScriptScope().getCompilerSettings().areRegexesEnabled() == false) {
            throw userRegexNode.createError(new IllegalStateException("Regexes are disabled. Set [script.painless.regex.enabled] to [true] "
                    + "in elasticsearch.yaml to allow them. Be careful though, regexes break out of Painless's protection against deep "
                    + "recursion and long loops."));
        }

        Location location = userRegexNode.getLocation();

        int constant = 0;

        for (int i = 0; i < flags.length(); ++i) {
            char flag = flags.charAt(i);

            switch (flag) {
                case 'c':
                    constant |= Pattern.CANON_EQ;
                    break;
                case 'i':
                    constant |= Pattern.CASE_INSENSITIVE;
                    break;
                case 'l':
                    constant |= Pattern.LITERAL;
                    break;
                case 'm':
                    constant |= Pattern.MULTILINE;
                    break;
                case 's':
                    constant |= Pattern.DOTALL;
                    break;
                case 'U':
                    constant |= Pattern.UNICODE_CHARACTER_CLASS;
                    break;
                case 'u':
                    constant |= Pattern.UNICODE_CASE;
                    break;
                case 'x':
                    constant |= Pattern.COMMENTS;
                    break;
                default:
                    throw new IllegalArgumentException("invalid regular expression: unknown flag [" + flag + "]");
            }
        }

        try {
            Pattern.compile(pattern, constant);
        } catch (PatternSyntaxException e) {
            throw new Location(location.getSourceName(), location.getOffset() + 1 + e.getIndex()).createError(
                    new IllegalArgumentException("Error compiling regex: " + e.getDescription()));
        }

        semanticScope.putDecoration(userRegexNode, new ValueType(Pattern.class));
        semanticScope.putDecoration(userRegexNode, new StandardConstant(flags));
    }
}
