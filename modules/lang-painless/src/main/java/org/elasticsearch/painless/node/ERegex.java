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

import org.elasticsearch.painless.Constant;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.WriterConstants;

/**
 * Represents a regex constant. All regexes are constants.
 */
public final class ERegex extends AExpression {

    private final String pattern;
    private final int flags;
    private Constant constant;

    public ERegex(Location location, String pattern, String flagsString) {
        super(location);

        this.pattern = pattern;

        int flags = 0;

        for (int c = 0; c < flagsString.length(); c++) {
            flags |= flagForChar(flagsString.charAt(c));
        }

        this.flags = flags;
    }

    @Override
    void extractVariables(Set<String> variables) {
        // Do nothing.
    }

    @Override
    void analyze(Locals locals) {
        if (!read) {
            throw createError(new IllegalArgumentException("Regex constant may only be read [" + pattern + "]."));
        }

        try {
            Pattern.compile(pattern, flags);
        } catch (PatternSyntaxException exception) {
            throw createError(exception);
        }

        constant = new Constant(location, Definition.PATTERN_TYPE.type, "regexAt$" + location.getOffset(), this::initializeConstant);
        actual = Definition.PATTERN_TYPE;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        writer.getStatic(WriterConstants.CLASS_TYPE, constant.name, Definition.PATTERN_TYPE.type);
        globals.addConstantInitializer(constant);
    }

    private void initializeConstant(MethodWriter writer) {
        writer.push(pattern);
        writer.push(flags);
        writer.invokeStatic(Definition.PATTERN_TYPE.type, WriterConstants.PATTERN_COMPILE);
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
