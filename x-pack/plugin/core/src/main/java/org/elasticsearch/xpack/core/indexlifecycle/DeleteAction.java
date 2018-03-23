/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * A {@link LifecycleAction} which deletes the index.
 */
public class DeleteAction implements LifecycleAction {
    public static final String NAME = "delete";

    private static final Logger logger = ESLoggerFactory.getLogger(DeleteAction.class);
    private static final ObjectParser<DeleteAction, Void> PARSER = new ObjectParser<>(NAME, DeleteAction::new);

    public static DeleteAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public DeleteAction() {
    }

    public DeleteAction(StreamInput in) throws IOException {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean indexSurvives() {
        return false;
    }

    @Override
    public List<Step> toSteps(String phase, Index index, Client client, ThreadPool threadPool, LongSupplier nowSupplier) {
        String indexName = index.getName();
        return Collections.singletonList(new ClientStep<DeleteIndexRequestBuilder, DeleteIndexResponse>( "delete",
            NAME, phase, indexName, client.admin().indices().prepareDelete(indexName),
            clusterState -> clusterState.metaData().hasIndex(indexName), response -> true));
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
