/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * An implementation of {@link ValueFetcher} that knows how to extract values
 * from the document source. Most standard field mappers will use this class
 * to implement value fetching.
 *
 * Field types that handle arrays directly should instead use {@link ArraySourceValueFetcher}.
 */
public abstract class SourceValueFetcher implements ValueFetcher {
    private final Set<String> sourcePaths;
    private final @Nullable Object nullValue;

    public SourceValueFetcher(String fieldName, SearchExecutionContext context) {
        this(fieldName, context, null);
    }

    /**
     * @param context   The query shard context
     * @param nullValue An optional substitute value if the _source value is 'null'.
     */
    public SourceValueFetcher(String fieldName, SearchExecutionContext context, Object nullValue) {
        this(context.sourcePath(fieldName), nullValue);
    }

    /**
     * @param sourcePaths   The paths to pull source values from
     * @param nullValue     An optional substitute value if the _source value is `null`
     */
    public SourceValueFetcher(Set<String> sourcePaths, Object nullValue) {
        this.sourcePaths = sourcePaths;
        this.nullValue = nullValue;
    }

    @Override
    public List<Object> fetchValues(SourceLookup lookup, List<Object> ignoredValues) {
        List<Object> values = new ArrayList<>();
        for (String path : sourcePaths) {
            Object sourceValue = lookup.extractValue(path, nullValue);
            if (sourceValue == null) {
                continue;
            }

            // We allow source values to contain multiple levels of arrays, such as `"field": [[1, 2]]`.
            // So we need to unwrap these arrays before passing them on to be parsed.
            Queue<Object> queue = new ArrayDeque<>();
            queue.add(sourceValue);
            while (queue.isEmpty() == false) {
                Object value = queue.poll();
                if (value instanceof List) {
                    for (Object o : (List<?>) value) {
                        if (o != null) {
                            queue.add(o);
                        }
                    }
                } else {
                    try {
                        Object parsedValue = parseSourceValue(value);
                        if (parsedValue != null) {
                            values.add(parsedValue);
                        } else {
                            ignoredValues.add(value);
                        }
                    } catch (Exception e) {
                        ignoredValues.add(value);
                        // if we get a parsing exception here, that means that the
                        // value in _source would have also caused a parsing
                        // exception at index time and the value ignored.
                        // so ignore it here as well
                    }
                }
            }
        }
        return values;
    }

    /**
     * Given a value that has been extracted from a document's source, parse it into a standard
     * format. This parsing logic should closely mirror the value parsing in
     * {@link FieldMapper#parseCreateField} or {@link FieldMapper#parse}.
     */
    protected abstract Object parseSourceValue(Object value);

    /**
     * Creates a {@link SourceValueFetcher} that passes through source values unmodified.
     */
    public static SourceValueFetcher identity(String fieldName, SearchExecutionContext context, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + fieldName + "] doesn't support formats.");
        }
        return new SourceValueFetcher(fieldName, context) {
            @Override
            protected Object parseSourceValue(Object value) {
                return value;
            }
        };
    }

    /**
     * Creates a {@link SourceValueFetcher} that converts source values to strings.
     */
    public static SourceValueFetcher toString(String fieldName, SearchExecutionContext context, String format) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + fieldName + "] doesn't support formats.");
        }
        return new SourceValueFetcher(fieldName, context) {
            @Override
            protected Object parseSourceValue(Object value) {
                return value.toString();
            }
        };
    }

    /**
     * Creates a {@link SourceValueFetcher} that converts source values to Strings
     * @param sourcePaths   the paths to fetch values from in the source
     */
    public static SourceValueFetcher toString(Set<String> sourcePaths) {
        return new SourceValueFetcher(sourcePaths, null) {
            @Override
            protected Object parseSourceValue(Object value) {
                return value.toString();
            }
        };
    }
}
