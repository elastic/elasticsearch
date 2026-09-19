/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Implements source filtering based on a list of included and excluded fields.  To use,
 * construct a SourceFilter and pass it to {@link Source#filter(SourceFilter)}
 */
public final class SourceFilter {

    private Function<Map<String, Object>, Map<String, Object>> mapFilter = null;
    private Function<Source, Source> bytesFilter = null;

    private final boolean canFilterBytes;
    private final boolean empty;
    private final String[] includes;
    private final String[] excludes;
    private CharacterRunAutomaton includeAut;
    private CharacterRunAutomaton excludeAut;
    private boolean includesUncompilable;
    private boolean excludesUncompilable;
    private IllegalArgumentException includesCompileException;
    private IllegalArgumentException excludesCompileException;
    private int pathFilteredCount;

    /**
     * Construct a new filter based on a list of includes and excludes
     * @param includes  an array of fields to include (may be null)
     * @param excludes  an array of fields to exclude (may be null)
     */
    public SourceFilter(String[] includes, String[] excludes) {
        this.includes = includes == null ? Strings.EMPTY_ARRAY : includes;
        this.excludes = excludes == null ? Strings.EMPTY_ARRAY : excludes;
        // TODO: Remove this once we upgrade to Jackson 2.14. There is currently a bug
        // in exclude filtering if one of the excludes contains a wildcard '*'.
        // see https://github.com/FasterXML/jackson-core/pull/729
        this.canFilterBytes = CollectionUtils.isEmpty(excludes) || Arrays.stream(excludes).noneMatch(field -> field.contains("*"));
        this.empty = CollectionUtils.isEmpty(this.includes) && CollectionUtils.isEmpty(this.excludes);
    }

    public String[] getIncludes() {
        return includes;
    }

    public String[] getExcludes() {
        return excludes;
    }

    /**
     * Checks if the given path matches at least one explicitly defined include pattern.
     * <p>
     * If no include patterns are defined, this method always returns {@code false}.
     *
     * @param fullPath the full path to evaluate
     * @return {@code true} if the path matches any explicitly defined include pattern,
     *         {@code false} otherwise
     */
    public boolean isExplicitlyIncluded(String fullPath) {
        if (includes.length == 0) {
            return false;
        }
        CharacterRunAutomaton aut = compileIncludes();
        if (aut == null) {
            return matchesPathOrAncestor(includes, fullPath);
        }
        int state = step(aut, fullPath, 0);
        return state != -1 && aut.isAccept(state);
    }

    /**
     * Determines whether the given full path should be filtered out.
     *
     * @param fullPath The full path to evaluate.
     * @param isObject Indicates if the path represents an object.
     * @return {@code true} if the path should be filtered out, {@code false} otherwise.
     */
    public boolean isPathFiltered(String fullPath, boolean isObject) {
        pathFilteredCount++;
        final boolean included;
        if (includes.length > 0) {
            CharacterRunAutomaton aut = compileIncludes();
            if (aut == null) {
                // Object semantics ("could any pattern match a descendant?") are not needed by the
                // vector-walk callers, which pass isObject=false. Callers that pass true keep today's
                // behaviour, now as a 400 instead of a 500.
                if (isObject) {
                    throw includesCompileException;
                }
                included = matchesPathOrAncestor(includes, fullPath);
            } else {
                int state = step(aut, fullPath, 0);
                included = state != -1 && (isObject || aut.isAccept(state));
            }
        } else {
            included = true;
        }

        if (excludes.length > 0) {
            CharacterRunAutomaton aut = compileExcludes();
            if (aut == null) {
                if (isObject) {
                    throw excludesCompileException;
                }
                if (matchesPathOrAncestor(excludes, fullPath)) {
                    return true;
                }
            } else {
                int state = step(aut, fullPath, 0);
                if (state != -1 && aut.isAccept(state)) {
                    return true;
                }
            }
        }

        return included == false;
    }

    private CharacterRunAutomaton compileIncludes() {
        if (includeAut != null) {
            return includeAut;
        }
        if (includesUncompilable) {
            return null;
        }
        try {
            includeAut = XContentMapValues.compileAutomaton(includes, new CharacterRunAutomaton(Automata.makeAnyString()));
            return includeAut;
        } catch (IllegalArgumentException e) {
            includesUncompilable = true;
            includesCompileException = e;
            return null;
        }
    }

    private CharacterRunAutomaton compileExcludes() {
        if (excludeAut != null) {
            return excludeAut;
        }
        if (excludesUncompilable) {
            return null;
        }
        try {
            excludeAut = XContentMapValues.compileAutomaton(excludes, new CharacterRunAutomaton(Automata.makeEmpty()));
            return excludeAut;
        } catch (IllegalArgumentException e) {
            excludesUncompilable = true;
            excludesCompileException = e;
            return null;
        }
    }

    /**
     * The language accepted by the compiled automaton plus its {@code ("" | "." .*)} tail, restricted
     * to leaf paths.
     */
    private static boolean matchesPathOrAncestor(String[] patterns, String fullPath) {
        if (patterns.length == 0) {
            return false;
        }
        if (Regex.simpleMatch(patterns, fullPath)) {
            return true;
        }
        for (int dot = fullPath.indexOf('.'); dot >= 0; dot = fullPath.indexOf('.', dot + 1)) {
            if (Regex.simpleMatch(patterns, fullPath.substring(0, dot))) {
                return true;
            }
        }
        return false;
    }

    /** Package-private for tests that assert the synthetic-vectors walk does not probe every mapper. */
    int pathFilteredCount() {
        return pathFilteredCount;
    }

    private static int step(CharacterRunAutomaton automaton, String key, int state) {
        // Step by code point, not UTF-16 char: the automaton is built over code points (see CharacterRunAutomaton#run).
        for (int i = 0; state != -1 && i < key.length();) {
            final int cp = key.codePointAt(i);
            state = automaton.step(state, cp);
            i += Character.charCount(cp);
        }
        return state;
    }

    /**
     * Filter a Source using its map representation
     */
    public Source filterMap(Source in) {
        if (this.empty) {
            return in;
        }
        if (mapFilter == null) {
            mapFilter = XContentMapValues.filter(includes, excludes);
        }
        return Source.fromMap(mapFilter.apply(in.source()), in.sourceContentType());
    }

    /**
     * Filter a Source using its bytes representation
     */
    public Source filterBytes(Source in) {
        if (this.empty) {
            return in;
        }
        if (bytesFilter == null) {
            bytesFilter = buildBytesFilter();
        }
        return bytesFilter.apply(in);
    }

    private Function<Source, Source> buildBytesFilter() {
        if (canFilterBytes == false) {
            return this::filterMap;
        }
        final XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withFiltering(
            null,
            Set.copyOf(Arrays.asList(includes)),
            Set.copyOf(Arrays.asList(excludes)),
            true
        );
        return in -> {
            try {
                BytesStreamOutput streamOutput = new BytesStreamOutput(1024);
                XContent xContent = in.sourceContentType().xContent();
                XContentBuilder builder = new XContentBuilder(xContent, streamOutput);
                try (
                    XContentParser parser = XContentHelper.createParserNotCompressed(parserConfig, in.internalSourceRef(), xContent.type())
                ) {
                    if ((parser.currentToken() == null) && (parser.nextToken() == null)) {
                        return Source.empty(in.sourceContentType());
                    }
                    builder.copyCurrentStructure(parser);
                    return Source.fromBytes(BytesReference.bytes(builder));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    public boolean excludesAll() {
        return Arrays.asList(excludes).contains("*");
    }
}
