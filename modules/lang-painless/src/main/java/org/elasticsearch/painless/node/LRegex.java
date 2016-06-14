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
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Constant;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.WriterConstants;

/**
 * Represents a regex constant. All regexes are constants.
 */
public final class LRegex extends ALink {
    private final String pattern;
    private Constant constant;

    public LRegex(Location location, String pattern) {
        super(location, 1);
        this.pattern = pattern;
        try {
            // Compile the pattern early after parsing so we can throw an error to the user with the location
            Pattern.compile(pattern);
        } catch (PatternSyntaxException e) {
            throw createError(e);
        }
    }

    @Override
    ALink analyze(Locals locals) {
        if (before != null) {
            throw createError(new IllegalArgumentException("Illegal Regex constant [" + pattern + "]."));
        } else if (store) {
            throw createError(new IllegalArgumentException("Cannot write to Regex constant [" + pattern + "]."));
        } else if (!load) {
            throw createError(new IllegalArgumentException("Regex constant may only be read [" + pattern + "]."));
        }

        constant = locals.addConstant(location, Definition.PATTERN_TYPE, "regexAt$" + location.getOffset(), this::initializeConstant);
        after = Definition.PATTERN_TYPE;

        return this;
    }

    @Override
    void write(MethodWriter writer) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer) {
        writer.writeDebugInfo(location);
        writer.getStatic(WriterConstants.CLASS_TYPE, constant.name, Definition.PATTERN_TYPE.type);
    }

    @Override
    void store(MethodWriter writer) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    private void initializeConstant(MethodWriter writer) {
        writer.push(pattern);
        writer.invokeStatic(Definition.PATTERN_TYPE.type, WriterConstants.PATTERN_COMPILE);
    }
}
