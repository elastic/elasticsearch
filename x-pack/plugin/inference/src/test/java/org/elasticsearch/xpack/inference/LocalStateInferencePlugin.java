/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.inference.InferenceServiceExtension;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.inference.highlight.SemanticTextHighlighter;
import org.elasticsearch.xpack.inference.mock.TestDenseInferenceServiceExtension;
import org.elasticsearch.xpack.inference.mock.TestSparseInferenceServiceExtension;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class LocalStateInferencePlugin extends LocalStateCompositeXPackPlugin {
    private final InferencePlugin inferencePlugin;

    public LocalStateInferencePlugin(final Settings settings, final Path configPath) throws Exception {
        super(settings, configPath);
        LocalStateInferencePlugin thisVar = this;
        this.inferencePlugin = new InferencePlugin(settings) {
            @Override
            protected SSLService getSslService() {
                return thisVar.getSslService();
            }

            @Override
            protected XPackLicenseState getLicenseState() {
                return thisVar.getLicenseState();
            }

            @Override
            public List<InferenceServiceExtension.Factory> getInferenceServiceFactories() {
                return List.of(
                    TestSparseInferenceServiceExtension.TestInferenceService::new,
                    TestDenseInferenceServiceExtension.TestInferenceService::new
                );
            }
        };
        plugins.add(inferencePlugin);
    }

    @Override
    public List<RetrieverSpec<?>> getRetrievers() {
        return this.filterPlugins(SearchPlugin.class).stream().flatMap(p -> p.getRetrievers().stream()).collect(toList());
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return inferencePlugin.getMappers();
    }

    @Override
    public Map<String, Highlighter> getHighlighters() {
        return Map.of(SemanticTextHighlighter.NAME, new SemanticTextHighlighter());
    }

    @Override
    public Collection<MappedActionFilter> getMappedActionFilters() {
        return inferencePlugin.getMappedActionFilters();
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return inferencePlugin.getQueries();
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(super.getNamedWriteables());
        namedWriteables.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(QueryBuilder.class, SparseVectorQueryBuilder.NAME, SparseVectorQueryBuilder::new)
        );

        return namedWriteables;
    }
}
