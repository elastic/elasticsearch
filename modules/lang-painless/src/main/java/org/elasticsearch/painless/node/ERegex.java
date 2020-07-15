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

    @Override
    void analyze(SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to regex constant [" + pattern + "] with flags [" + flags + "]"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: regex constant [" + pattern + "] with flags [" + flags + "] not used"));
        }

        if (semanticScope.getScriptScope().getCompilerSettings().areRegexesEnabled() == false) {
            throw createError(new IllegalStateException("Regexes are disabled. Set [script.painless.regex.enabled] to [true] "
                    + "in elasticsearch.yaml to allow them. Be careful though, regexes break out of Painless's protection against deep "
                    + "recursion and long loops."));
        }

        int flags = 0;

        for (int c = 0; c < this.flags.length(); c++) {
            flags |= flagForChar(this.flags.charAt(c));
        }

        try {
            Pattern.compile(pattern, flags);
        } catch (PatternSyntaxException e) {
            throw new Location(getLocation().getSourceName(), getLocation().getOffset() + 1 + e.getIndex()).createError(
                    new IllegalArgumentException("Error compiling regex: " + e.getDescription()));
        }

        semanticScope.putDecoration(this, new ValueType(Pattern.class));
        semanticScope.putDecoration(this, new StandardConstant(flags));
    }

    private int flagForChar(char c) {
        switch (c) {
            case 'c': return Pattern.CANON_EQ;
            case 'i': return Pattern.CASE_INSENSITIVE;
            case 'l': return Pattern.LITERAL;
            case 'm': return Pattern.MULTILINE;
            case 's': return Pattern.DOTALL;
            case 'U': return Pattern.UNICODE_CHARACTER_CLASS;
            case 'u': return Pattern.UNICODE_CASE;
            case 'x': return Pattern.COMMENTS;
            default:
                throw new IllegalArgumentException("Unknown flag [" + c + "]");
        }
    }
}
