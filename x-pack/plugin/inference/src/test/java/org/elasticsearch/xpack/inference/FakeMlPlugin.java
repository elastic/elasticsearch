/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.xpack.core.ml.action.CoordinatedInferenceAction;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.vectors.TextEmbeddingQueryVectorBuilder;
import org.elasticsearch.xpack.ml.action.TransportCoordinatedInferenceAction;

import java.util.Collection;
import java.util.List;

public class FakeMlPlugin extends Plugin implements ActionPlugin, SearchPlugin {
    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return new MlInferenceNamedXContentProvider().getNamedWriteables();
    }

    @Override
    public List<QueryVectorBuilderSpec<?>> getQueryVectorBuilders() {
        return List.of(
            new QueryVectorBuilderSpec<>(
                TextEmbeddingQueryVectorBuilder.NAME,
                TextEmbeddingQueryVectorBuilder::new,
                TextEmbeddingQueryVectorBuilder.PARSER
            )
        );
    }

    @Override
    public Collection<ActionHandler> getActions() {
        return List.of(new ActionHandler(CoordinatedInferenceAction.INSTANCE, TransportCoordinatedInferenceAction.class));
    }
}
