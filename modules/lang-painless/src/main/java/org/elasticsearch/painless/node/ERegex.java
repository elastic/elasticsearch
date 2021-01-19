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

import java.util.Objects;

/**
 * Represents a regex constant. All regexes are constants.
 */
public class ERegex extends AExpression {

    private final Flavor flavor;
    private final int patternStart;
    private final String pattern;
    private final String flags;

    public ERegex(int identifier, Location location, Flavor flavor, int patternStart, String pattern, String flags) {
        super(identifier, location);

        this.flavor = Objects.requireNonNull(flavor);
        this.patternStart = patternStart;
        this.pattern = Objects.requireNonNull(pattern);
        this.flags = Objects.requireNonNull(flags);
    }

    /**
     * The 
     */
    public Flavor getFlavor() {
        return flavor;
    }

    /**
     * Offset from {@link #getLocation()} where the pattern starts.
     */
    public int getPatternStart() {
        return patternStart;
    }

    public String getPattern() {
        return pattern;
    }

    public String getFlags() {
        return flags;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitRegex(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        // terminal node; no children
    }

    public enum Flavor {
        JAVA,
        GROK,
        DISECT;

        public static Flavor parse(String flavor) {
            switch (flavor) {
                case "":
                    return JAVA;
                case "g":
                    return GROK;
                case "d":
                    return DISECT;
                default:
                    throw new IllegalArgumentException("Unsupported regex flavor [" + flavor + "]. Must be unspecified or one of [dg]");
            }
        }
    }
}
