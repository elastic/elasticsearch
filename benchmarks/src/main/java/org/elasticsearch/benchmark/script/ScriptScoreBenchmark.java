/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.script;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.DocValuesDocReader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceProvider;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A race between Lucene Expressions, Painless, and a hand optimized script
 * implementing a {@link ScriptScoreQuery}.
 */
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@OperationsPerInvocation(1_000_000)   // The index has a million documents in it.
@State(Scope.Benchmark)
public class ScriptScoreBenchmark {
    private final PluginsService pluginsService = new PluginsService(
        Settings.EMPTY,
        null,
        null,
        Path.of(System.getProperty("plugins.dir"))
    );
    private final ScriptModule scriptModule = new ScriptModule(Settings.EMPTY, pluginsService.filterPlugins(ScriptPlugin.class));

    private final Map<String, MappedFieldType> fieldTypes = Map.ofEntries(
        Map.entry("n", new NumberFieldType("n", NumberType.LONG, false, false, true, true, null, Map.of(), null, false, null, null))
    );
    private final IndexFieldDataCache fieldDataCache = new IndexFieldDataCache.None();
    private final CircuitBreakerService breakerService = new NoneCircuitBreakerService();
    private final SearchLookup lookup = new SearchLookup(
        fieldTypes::get,
        (mft, lookup, fdo) -> mft.fielddataBuilder(FieldDataContext.noRuntimeFields("benchmark")).build(fieldDataCache, breakerService),
        SourceProvider.fromStoredFields()
    );

    @Param({ "expression", "metal", "painless_cast", "painless_def" })
    private String script;

    @Param({ "16" })
    private double indexingBufferMb;

    private ScoreScript.Factory factory;

    private IndexReader reader;

    @Setup
    public void setupScript() {
        factory = switch (script) {
            case "expression" -> scriptModule.engines.get("expression").compile("test", "doc['n'].value", ScoreScript.CONTEXT, Map.of());
            case "metal" -> bareMetalScript();
            case "painless_cast" -> scriptModule.engines.get("painless")
                .compile(
                    "test",
                    "((org.elasticsearch.index.fielddata.ScriptDocValues.Longs)doc['n']).value",
                    ScoreScript.CONTEXT,
                    Map.of()
                );
            case "painless_def" -> scriptModule.engines.get("painless").compile("test", "doc['n'].value", ScoreScript.CONTEXT, Map.of());
            default -> throw new IllegalArgumentException("Don't know how to implement script [" + script + "]");
        };
    }

    @Setup
    public void setupIndex() throws IOException {
        Path path = Path.of(System.getProperty("tests.index"));
        IOUtils.rm(path);
        Directory directory = new MMapDirectory(path);
        try (
            IndexWriter w = new IndexWriter(
                directory,
                new IndexWriterConfig().setOpenMode(OpenMode.CREATE).setRAMBufferSizeMB(indexingBufferMb)
            )
        ) {
            for (int i = 1; i <= 1_000_000; i++) {
                w.addDocument(List.of(new SortedNumericDocValuesField("n", i)));
            }
            w.commit();
        }
        reader = DirectoryReader.open(directory);
    }

    @Benchmark
    public TopDocs benchmark() throws IOException {
        TopDocs topDocs = new IndexSearcher(reader).search(scriptScoreQuery(factory), 10);
        if (topDocs.scoreDocs[0].score != 1_000_000) {
            throw new AssertionError("Expected score to be 1,000,000 but was [" + topDocs.scoreDocs[0].score + "]");
        }
        return topDocs;
    }

    private Query scriptScoreQuery(ScoreScript.Factory factory) {
        ScoreScript.LeafFactory leafFactory = factory.newFactory(Map.of(), lookup);
        return new ScriptScoreQuery(new MatchAllDocsQuery(), null, leafFactory, lookup, null, "test", 0, IndexVersion.CURRENT);
    }

    private ScoreScript.Factory bareMetalScript() {
        return (params, lookup) -> {
            MappedFieldType type = fieldTypes.get("n");
            IndexNumericFieldData ifd = (IndexNumericFieldData) lookup.getForField(type, MappedFieldType.FielddataOperation.SEARCH);
            return new ScoreScript.LeafFactory() {
                @Override
                public ScoreScript newInstance(DocReader docReader) throws IOException {
                    SortedNumericDocValues values = ifd.load(((DocValuesDocReader) docReader).getLeafReaderContext()).getLongValues();
                    return new ScoreScript(params, null, docReader) {
                        private int docId;

                        @Override
                        public double execute(ExplanationHolder explanation) {
                            try {
                                values.advance(docId);
                                if (values.docValueCount() != 1) {
                                    throw new IllegalArgumentException("script only works when there is exactly one value");
                                }
                                return values.nextValue();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public void setDocument(int docid) {
                            this.docId = docid;
                        }
                    };
                }

                @Override
                public boolean needs_score() {
                    return false;
                }
            };
        };
    }
}
