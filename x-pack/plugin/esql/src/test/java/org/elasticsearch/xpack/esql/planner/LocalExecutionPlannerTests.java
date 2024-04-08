/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.LuceneTopNSourceOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.xpack.esql.TestBlockFactory;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.hamcrest.Matcher;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LocalExecutionPlannerTests extends MapperServiceTestCase {
    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[] { false });
        params.add(new Object[] { true });
        return params;
    }

    private final QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);
    private final boolean estimatedRowSizeIsHuge;

    private Directory directory = newDirectory();
    private IndexReader reader;

    private final ArrayList<Releasable> releasables = new ArrayList<>();

    public LocalExecutionPlannerTests(@Name("estimatedRowSizeIsHuge") boolean estimatedRowSizeIsHuge) {
        this.estimatedRowSizeIsHuge = estimatedRowSizeIsHuge;
    }

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory, () -> Releasables.close(releasables), releasables::clear);
    }

    public void testLuceneSourceOperatorHugeRowSize() throws IOException {
        int estimatedRowSize = randomEstimatedRowSize(estimatedRowSizeIsHuge);
        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan(
            new EsQueryExec(Source.EMPTY, index(), List.of(), null, null, null, estimatedRowSize)
        );
        assertThat(plan.driverFactories.size(), lessThanOrEqualTo(pragmas.taskConcurrency()));
        LocalExecutionPlanner.DriverSupplier supplier = plan.driverFactories.get(0).driverSupplier();
        var factory = (LuceneSourceOperator.Factory) supplier.physicalOperation().sourceOperatorFactory;
        assertThat(factory.maxPageSize(), maxPageSizeMatcher(estimatedRowSizeIsHuge, estimatedRowSize));
        assertThat(factory.limit(), equalTo(Integer.MAX_VALUE));
    }

    public void testLuceneTopNSourceOperator() throws IOException {
        int estimatedRowSize = randomEstimatedRowSize(estimatedRowSizeIsHuge);
        FieldAttribute sortField = new FieldAttribute(Source.EMPTY, "field", new EsField("field", DataTypes.INTEGER, Map.of(), true));
        EsQueryExec.FieldSort sort = new EsQueryExec.FieldSort(sortField, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        Literal limit = new Literal(Source.EMPTY, 10, DataTypes.INTEGER);
        LocalExecutionPlanner.LocalExecutionPlan plan = planner().plan(
            new EsQueryExec(Source.EMPTY, index(), List.of(), null, limit, List.of(sort), estimatedRowSize)
        );
        assertThat(plan.driverFactories.size(), lessThanOrEqualTo(pragmas.taskConcurrency()));
        LocalExecutionPlanner.DriverSupplier supplier = plan.driverFactories.get(0).driverSupplier();
        var factory = (LuceneTopNSourceOperator.Factory) supplier.physicalOperation().sourceOperatorFactory;
        assertThat(factory.maxPageSize(), maxPageSizeMatcher(estimatedRowSizeIsHuge, estimatedRowSize));
        assertThat(factory.limit(), equalTo(10));
    }

    private int randomEstimatedRowSize(boolean huge) {
        int hugeBoundary = SourceOperator.MIN_TARGET_PAGE_SIZE * 10;
        return huge ? between(hugeBoundary, Integer.MAX_VALUE) : between(1, hugeBoundary);
    }

    private Matcher<Integer> maxPageSizeMatcher(boolean estimatedRowSizeIsHuge, int estimatedRowSize) {
        if (estimatedRowSizeIsHuge) {
            return equalTo(SourceOperator.MIN_TARGET_PAGE_SIZE);
        }
        return equalTo(SourceOperator.TARGET_PAGE_SIZE / estimatedRowSize);
    }

    private LocalExecutionPlanner planner() throws IOException {
        return new LocalExecutionPlanner(
            "test",
            "",
            null,
            BigArrays.NON_RECYCLING_INSTANCE,
            TestBlockFactory.getNonBreakingInstance(),
            Settings.EMPTY,
            config(),
            null,
            null,
            null,
            esPhysicalOperationProviders()
        );
    }

    private EsqlConfiguration config() {
        return new EsqlConfiguration(
            randomZone(),
            randomLocale(random()),
            "test_user",
            "test_cluser",
            pragmas,
            EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(null),
            EsqlPlugin.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(null),
            StringUtils.EMPTY,
            false
        );
    }

    private EsPhysicalOperationProviders esPhysicalOperationProviders() throws IOException {
        int numShards = randomIntBetween(1, 1000);
        List<EsPhysicalOperationProviders.ShardContext> shardContexts = new ArrayList<>(numShards);
        var searcher = new ContextIndexSearcher(
            reader(),
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            TrivialQueryCachingPolicy.NEVER,
            true
        );
        for (int i = 0; i < numShards; i++) {
            shardContexts.add(
                new EsPhysicalOperationProviders.DefaultShardContext(
                    i,
                    createSearchExecutionContext(createMapperService(mapping(b -> {})), searcher),
                    null
                )
            );
        }
        releasables.add(searcher);
        return new EsPhysicalOperationProviders(shardContexts);
    }

    private IndexReader reader() {
        if (reader != null) {
            return reader;
        }
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int d = 0; d < 10; d++) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(new SortedNumericDocValuesField("s", d));
                writer.addDocument(doc);
            }
            reader = writer.getReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return reader;
    }

    private EsIndex index() {
        return new EsIndex("test", Map.of());
    }
}
