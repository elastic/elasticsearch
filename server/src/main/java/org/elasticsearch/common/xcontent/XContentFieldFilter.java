/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A filter that filter fields away from source
 */
public interface XContentFieldFilter {
    /**
     * filter source in {@link BytesReference} format and in {@link XContentType} content type
     * note that xContentType may be null in some case, we should guess xContentType from sourceBytes in such cases
     */
    BytesReference apply(BytesReference sourceBytes, @Nullable XContentType xContentType) throws IOException;

    /**
     * Construct {@link XContentFieldFilter} using given includes and excludes
     *
     * @param includes fields to keep, wildcard supported
     * @param excludes fields to remove, wildcard supported
     * @return filter using {@link XContentMapValues#filter(String[], String[])} if wildcard found in excludes
     *         , otherwise return filter using {@link XContentParser}
     */
    static XContentFieldFilter newFieldFilter(String[] includes, String[] excludes) {
        if ((CollectionUtils.isEmpty(excludes) == false) && Arrays.stream(excludes).filter(field -> field.contains("*")).count() > 0) {
            return (originalSource, contentType) -> {
                Function<Map<String, ?>, Map<String, Object>> mapFilter = XContentMapValues.filter(includes, excludes);
                Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(originalSource, true, contentType);
                Map<String, Object> filteredSource = mapFilter.apply(mapTuple.v2());
                BytesStreamOutput bStream = new BytesStreamOutput();
                XContentType actualContentType = mapTuple.v1();
                XContentBuilder builder = XContentFactory.contentBuilder(actualContentType, bStream).map(filteredSource);
                builder.close();
                return bStream.bytes();
            };
        } else {
            final XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withFiltering(
                Set.of(includes),
                Set.of(excludes)
            );
            return (originalSource, contentType) -> {
                if (contentType == null) {
                    contentType = XContentHelper.xContentTypeMayCompressed(originalSource);
                }
                BytesStreamOutput streamOutput = new BytesStreamOutput(Math.min(1024, originalSource.length()));
                XContentBuilder builder = new XContentBuilder(contentType.xContent(), streamOutput);
                XContentParser parser = contentType.xContent().createParser(parserConfig, originalSource.streamInput());
                builder.copyCurrentStructure(parser);
                return BytesReference.bytes(builder);
            };
        }
    }
}
