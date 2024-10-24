/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.lucene.search.AutomatonQueries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.WeakHashMap;

public class IndexResolver /*implements ClusterStateListener*/ {

    private static final String NAME_FIELD = "name";
    private static final String HIDDEN_FIELD = "hidden";
    private static final String SYSTEM_FIELD = "system";
    private static final String TYPE_FIELD = "type";
    private static final String ALIAS_FIELD = "alias";
    private static final String OPEN_CLOSED_FIELD = "open_closed";
    private static final String BACKING_INDEX_FIELD = "backing_index";
    private static final String DATA_STREAM_FIELD = "data_stream";

    private WeakHashMap<SortedMap<String, IndexAbstraction>, IndexSearcher> lookupDirectories = new WeakHashMap<>();

    // @Override
    // public void clusterChanged(ClusterChangedEvent event) {
    // if (event.metadataChanged()
    // && event.previousState().metadata().getIndicesLookup().equals(event.state().metadata().getIndicesLookup()) == false) {
    // Directory dir = buildIndex(event.state());
    // synchronized (lookupDirectories) {
    // lookupDirectories.put(event.state().getMetadata().getIndicesLookup(), dir);
    // }
    // }
    // }

    private static void addBackingIndices(Document doc, IndexAbstraction abstraction) {
        for (var idx : abstraction.getIndices()) {
            doc.add(new KeywordField(BACKING_INDEX_FIELD, idx.getName(), Field.Store.YES));
        }
    }

    private static void addConcreteIndexInfo(Document doc, Metadata metadata, IndexAbstraction.ConcreteIndex concreteIndex) {
        if (concreteIndex.getParentDataStream() != null) {
            doc.add(new KeywordField(DATA_STREAM_FIELD, concreteIndex.getParentDataStream().getName(), Field.Store.YES));
        }
        IndexMetadata indexMetadata = metadata.index(concreteIndex.getWriteIndex());
        Map<String, AliasMetadata> aliasMetadatas = indexMetadata.getAliases();
        if (aliasMetadatas != null && aliasMetadatas.isEmpty() == false) {
            for (var aliasMd : aliasMetadatas.values()) {
                doc.add(new KeywordField(ALIAS_FIELD, aliasMd.getAlias(), Field.Store.NO));
            }
        }
        doc.add(new KeywordField(OPEN_CLOSED_FIELD, indexMetadata.getState().toString(), Field.Store.NO));

    }

    /**
     * Builds a Lucene index in an in
     * @param current
     * @return
     */
    // pkg-private for tests
    static Directory buildIndex(ClusterState current) {
        Directory dir = new ByteBuffersDirectory();
        // Quick n dirty analyzer to default to case-insensitive keyword analyzer
        Analyzer analyzer = new StandardAnalyzer();

        // Configure index writer
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer;
        try {
            writer = new IndexWriter(dir, config);

            for (var entry : current.metadata().getIndicesLookup().entrySet()) {
                Document indexInfo = new Document();
                IndexAbstraction idxAbstraction = entry.getValue();
                indexInfo.add(new KeywordField(NAME_FIELD, entry.getKey(), Field.Store.YES));
                indexInfo.add(new KeywordField(HIDDEN_FIELD, Boolean.toString(idxAbstraction.isHidden()), Field.Store.NO));
                indexInfo.add(new KeywordField(SYSTEM_FIELD, Boolean.toString(idxAbstraction.isSystem()), Field.Store.NO));
                indexInfo.add(new KeywordField(TYPE_FIELD, idxAbstraction.getType().getDisplayName(), Field.Store.NO));
                switch (idxAbstraction) {
                    case IndexAbstraction.ConcreteIndex concrete -> addConcreteIndexInfo(indexInfo, current.metadata(), concrete);
                    default -> addBackingIndices(indexInfo, idxAbstraction);
                }
                writer.addDocument(indexInfo);
            }
            writer.close();
            return dir;
        } catch (IOException e) {
            throw new RuntimeException("Failed to build in-memory structure of index metadata", e); // ATHE: not likely in memory?
        }

    }

    public static Collection<String> concreteIndexNames(IndexNameExpressionResolver.Context context, Collection<String> expressions) {
        return concreteIndexNames(context, expressions.toArray(new String[0]));
    }

    public static Collection<String> concreteIndexNames(IndexNameExpressionResolver.Context context, String... expressions) {

        try {
            IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(buildIndex(context.getState())));
            BooleanQuery.Builder topQuery = new BooleanQuery.Builder();
            if (expressions == null || expressions.length == 0) {
//                topQuery.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
            } else {
                IndexNameExpressionResolver.ExpressionList exps = new IndexNameExpressionResolver.ExpressionList(
                    context,
                    List.of(expressions)
                );
                for (var expression : exps) {
                    if (expression.get().equals("_all")) {
                        topQuery.add(new MatchAllDocsQuery(), BooleanClause.Occur.SHOULD);
                    } else if (expression.isWildcard()) {
                        if (expression.isExclusion()) {
                            topQuery.add(
                                AutomatonQueries.caseInsensitiveWildcardQuery(new Term(NAME_FIELD, expression.get().substring(0))),
                                BooleanClause.Occur.MUST_NOT
                            );
                        } else {
                            topQuery.add(
                                AutomatonQueries.caseInsensitiveWildcardQuery(new Term(NAME_FIELD, expression.get())),
                                BooleanClause.Occur.SHOULD
                            );
                        }
                    } else {
                        // it's not a wildcard
                        if (expression.isExclusion()) {
                            topQuery.add(
                                new TermQuery(new Term(NAME_FIELD, expression.get().substring(0))),
                                BooleanClause.Occur.MUST_NOT
                            );
                        } else {
                            topQuery.add(
                                new TermQuery(new Term(NAME_FIELD, expression.get())),
                                BooleanClause.Occur.SHOULD
                            );
                        }

                    }
                }
            }


            if (context.includeDataStreams() == false) {
                topQuery.add(
                    new TermQuery(new Term(TYPE_FIELD, IndexAbstraction.Type.DATA_STREAM.getDisplayName())),
                    BooleanClause.Occur.MUST_NOT
                );
                topQuery.add(
                    new FieldExistsQuery(DATA_STREAM_FIELD),
                    BooleanClause.Occur.MUST_NOT
                );
            }

            var options = context.getOptions();
            if (options.ignoreAliases()) {
                topQuery.add(
                    new TermQuery(new Term(TYPE_FIELD, IndexAbstraction.Type.ALIAS.getDisplayName())),
                    BooleanClause.Occur.MUST_NOT
                );
            }

            if (options.expandWildcardsClosed() == false) {
                topQuery.add(
                    new TermQuery(new Term(OPEN_CLOSED_FIELD, IndexMetadata.State.CLOSE.toString())),
                    BooleanClause.Occur.MUST_NOT
                );
            }

            if (options.expandWildcardsOpen() == false) {
                topQuery.add(new TermQuery(new Term(OPEN_CLOSED_FIELD, IndexMetadata.State.OPEN.toString())), BooleanClause.Occur.MUST_NOT);
            }

            if (options.expandWildcardsHidden() == false) {
                topQuery.add(new TermQuery(new Term(HIDDEN_FIELD, Boolean.TRUE.toString())), BooleanClause.Occur.MUST_NOT);
            }

            TopDocs results = searcher.search(topQuery.build(), Integer.MAX_VALUE); // ATHE: MAX_VALUE maybe not what we want here
            ArrayList<String> names = new ArrayList<>();
            for (var doc : results.scoreDocs) {
                names.add(searcher.storedFields().document(doc.doc).get(NAME_FIELD));
            }
            return names;
        } catch (IOException e) {
            throw new RuntimeException("Failed to build searcher for index metadata", e);
        }
    }
}
