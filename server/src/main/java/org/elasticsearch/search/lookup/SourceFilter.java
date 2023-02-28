/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.lookup;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
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
            Set.copyOf(Arrays.asList(includes)),
            Set.copyOf(Arrays.asList(excludes)),
            true
        );
        return in -> {
            try {
                BytesStreamOutput streamOutput = new BytesStreamOutput(1024);
                XContent xContent = in.sourceContentType().xContent();
                XContentBuilder builder = new XContentBuilder(xContent, streamOutput);
                XContentParser parser = xContent.createParser(parserConfig, in.internalSourceRef().streamInput());
                if ((parser.currentToken() == null) && (parser.nextToken() == null)) {
                    return Source.empty(in.sourceContentType());
                }
                builder.copyCurrentStructure(parser);
                return Source.fromBytes(BytesReference.bytes(builder));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
