/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * TODO: Add docs
 */
public interface OperatorHandler<T> {
    String CONTENT = "content";

    String key();

    Collection<T> prepare(Object source) throws IOException;

    Optional<ClusterState> transformClusterState(
        Collection<T> requests,
        ClusterState.Builder clusterStateBuilder,
        ClusterState previous
    );

    default XContentParser mapToXContentParser(Map<String, ?> source) {
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            builder.map(source);
            return XContentFactory.xContent(builder.contentType())
                .createParser(XContentParserConfiguration.EMPTY, Strings.toString(builder));
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }
}
