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
     * Determines whether the given full path should be filtered out.
     *
     * @param fullPath The full path to evaluate.
     * @param isObject Indicates if the path represents an object.
     * @return {@code true} if the path should be filtered out, {@code false} otherwise.
     */
    public boolean isPathFiltered(String fullPath, boolean isObject) {
        final boolean included;
        if (includes != null) {
            if (includeAut == null) {
                includeAut = XContentMapValues.compileAutomaton(includes, new CharacterRunAutomaton(Automata.makeAnyString()));
            }
            int state = step(includeAut, fullPath, 0);
            included = state != -1 && (isObject || includeAut.isAccept(state));
        } else {
            included = true;
        }

        if (excludes != null) {
            if (excludeAut == null) {
                excludeAut = XContentMapValues.compileAutomaton(excludes, new CharacterRunAutomaton(Automata.makeEmpty()));
            }
            int state = step(excludeAut, fullPath, 0);
            if (state != -1 && excludeAut.isAccept(state)) {
                return true;
            }
        }

        return included == false;
    }

    private static int step(CharacterRunAutomaton automaton, String key, int state) {
        for (int i = 0; state != -1 && i < key.length(); ++i) {
            state = automaton.step(state, key.charAt(i));
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
