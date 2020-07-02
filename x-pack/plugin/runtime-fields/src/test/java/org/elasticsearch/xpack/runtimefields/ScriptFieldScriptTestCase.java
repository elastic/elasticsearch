/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin.ExtensionLoader;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class ScriptFieldScriptTestCase<F, V, DV, R> extends ESTestCase {
    private final Set<Integer> visited = new LinkedHashSet<>();
    private final List<Closeable> lazyClose = new ArrayList<>();
    private final ScriptService scriptService;

    public ScriptFieldScriptTestCase() {
        PainlessPlugin painlessPlugin = new PainlessPlugin();
        painlessPlugin.loadExtensions(new ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked") // We only ever load painless extensions here so it is fairly safe.
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return (List<T>) List.of(new RuntimeFieldsPainlessExtension());
            }
        });
        List<ScriptPlugin> scriptPlugins = new ArrayList<>();
        scriptPlugins.add(painlessPlugin); // TODO move painless to integration tests
        scriptPlugins.add(new RuntimeFields());
        scriptPlugins.addAll(extraScriptPlugins());
        ScriptModule scriptModule = new ScriptModule(Settings.EMPTY, scriptPlugins);
        scriptService = new ScriptService(Settings.EMPTY, scriptModule.engines, scriptModule.contexts);
    }

    protected List<ScriptPlugin> extraScriptPlugins() {
        return List.of();
    }

    protected abstract MappedFieldType[] fieldTypes();

    protected abstract ScriptContext<F> scriptContext();

    protected abstract V newValues(F factory, Map<String, Object> params, SearchLookup searchLookup) throws IOException;

    protected abstract CheckedFunction<LeafReaderContext, DV, IOException> docValuesBuilder(V values);

    protected abstract void readAllDocValues(DV docValues, int docId, Consumer<R> sync) throws IOException;

    protected final TestCase testCase(CheckedConsumer<RandomIndexWriter, IOException> indexBuilder) throws IOException {
        return new TestCase(indexBuilder);
    }

    protected class TestCase {
        private final MapperService mapperService = mock(MapperService.class);
        private final SearchLookup searchLookup;
        private final IndexSearcher searcher;

        private TestCase(CheckedConsumer<RandomIndexWriter, IOException> indexBuilder) throws IOException {
            for (MappedFieldType type : fieldTypes()) {
                when(mapperService.fieldType(type.name())).thenReturn(type);
            }
            Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup = ft -> ft.fielddataBuilder("test")
                .build(indexSettings(), ft, null, new NoneCircuitBreakerService(), mapperService);
            searchLookup = new SearchLookup(mapperService, fieldDataLookup);
            Directory directory = newDirectory();
            lazyClose.add(directory);
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                indexBuilder.accept(indexWriter);
                DirectoryReader reader = indexWriter.getReader();
                lazyClose.add(reader);
                searcher = newSearcher(reader);
            }
        }

        protected V script(String script) throws IOException {
            return script(new Script(script));
        }

        protected V testScript(String name) throws IOException {
            return script(new Script(ScriptType.INLINE, "test", name, Map.of()));
        }

        private V script(Script script) throws IOException {
            return newValues(scriptService.compile(script, scriptContext()), Map.of(), searchLookup);
        }

        protected List<R> collect(String script) throws IOException {
            return collect(new MatchAllDocsQuery(), script(script));
        }

        protected List<R> collect(V values) throws IOException {
            return collect(new MatchAllDocsQuery(), values);
        }

        protected List<R> collect(Query query, V values) throws IOException {
            // Now run the query and collect the results
            List<R> result = new ArrayList<>();
            CheckedFunction<LeafReaderContext, DV, IOException> docValuesBuilder = docValuesBuilder(values);
            searcher.search(query, new Collector() {
                @Override
                public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE_NO_SCORES;
                }

                @Override
                public LeafCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
                    DV docValues = docValuesBuilder.apply(ctx);
                    return new LeafCollector() {
                        @Override
                        public void setScorer(Scorable scorer) throws IOException {}

                        @Override
                        public void collect(int docId) throws IOException {
                            readAllDocValues(docValues, docId, result::add);
                        }
                    };
                }
            });
            resetVisitedDocIds();
            return result;
        }
    }

    private IndexSettings indexSettings() {
        return new IndexSettings(
            IndexMetadata.builder("_index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            Settings.EMPTY
        );
    }

    @After
    public void closeAll() throws IOException {
        Collections.reverse(lazyClose); // Close in the reverse order added so readers close before directory
        IOUtils.close(lazyClose);
    }

    protected final void onVisitDocId(LeafReaderContext ctx, int docId) {
        int rebased = ctx.docBase + docId;
        if (false == visited.add(rebased)) {
            throw new AssertionError("Visited [" + rebased + "] twice. Order before was " + visited);
        }
    }

    private void resetVisitedDocIds() {
        visited.clear();
    }
}
