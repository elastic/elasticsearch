/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler.CompiledSchema;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.compiler.SchemaCompiler;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser.CharParser;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema.Schema;

/**
 * Factory for creating {@link Parser} instances with a pre-compiled schema.
 *
 * <p>All parser instances share the same compiled schema for efficiency.
 * This factory is thread-safe.
 */
public class ParserFactory {

    private static final CompiledSchema compiledSchema = SchemaCompiler.compile(Schema.getInstance());

    private ParserFactory() {
        // Prevent instantiation
    }

    /**
     * Creates a new parser instance.
     *
     * @return a new {@link Parser} instance
     */
    public static Parser createParser() {
        return new CharParser(compiledSchema);
    }
}
