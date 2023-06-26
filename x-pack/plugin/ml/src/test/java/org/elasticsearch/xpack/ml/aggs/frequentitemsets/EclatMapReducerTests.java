/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSource.Field;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr.ItemSetMapReduceValueSourceTests.createKeywordFieldTestInstance;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

public class EclatMapReducerTests extends ESTestCase {

    private static Supplier<Boolean> doNotCancelSupplier = () -> false;

    public void testSimple() throws IOException {
        Field field1 = createKeywordFieldTestInstance("keyword1", 0);
        Field field2 = createKeywordFieldTestInstance("keyword2", 1);
        Field field3 = createKeywordFieldTestInstance("keyword3", 2);

        EclatMapReducer eclat = new EclatMapReducer(getTestName(), 0.1, 2, 10, true);

        HashBasedTransactionStore transactionStore = eclat.mapInit(mockBigArrays());
        eclat.map(mockOneDocument(List.of(tuple(field1, "f1-a"), tuple(field2, "f2-1"), tuple(field3, "f3-A"))), transactionStore);
        eclat.map(mockOneDocument(List.of(tuple(field1, "f1-a"), tuple(field2, "f2-1"), tuple(field3, "f3-B"))), transactionStore);
        eclat.map(mockOneDocument(List.of(tuple(field1, "f1-a"), tuple(field2, "f2-1"), tuple(field3, "f3-C"))), transactionStore);
        eclat.map(mockOneDocument(List.of(tuple(field1, "f1-a"), tuple(field2, "f2-1"), tuple(field3, "f3-D"))), transactionStore);
        eclat.map(mockOneDocument(List.of(tuple(field1, "f1-a"), tuple(field2, "f2-1"), tuple(field3, "f3-E"))), transactionStore);
        eclat.map(mockOneDocument(List.of(tuple(field1, "f1-b"), tuple(field2, "f2-1"), tuple(field3, "f3-F"))), transactionStore);
        eclat.map(mockOneDocument(List.of(tuple(field1, "f1-b"), tuple(field2, "f2-1"), tuple(field3, "f3-G"))), transactionStore);
        eclat.map(mockOneDocument(List.of(tuple(field1, "f1-c"), tuple(field2, "f2-1"), tuple(field3, "f3-H"))), transactionStore);
        eclat.map(mockOneDocument(List.of(tuple(field1, "f1-d"), tuple(field2, "f2-1"), tuple(field3, "f3-I"))), transactionStore);
        eclat.map(mockOneDocument(List.of(tuple(field1, "f1-b"), tuple(field2, "f2-1"), tuple(field3, "f3-J"))), transactionStore);
        eclat.map(mockOneDocument(List.of(tuple(field1, "f1-f"), tuple(field2, "f2-1"), tuple(field3, "f3-K"))), transactionStore);
        eclat.map(mockOneDocument(List.of(tuple(field1, "f1-a"), tuple(field2, "f2-1"), tuple(field3, "f3-L"))), transactionStore);

        EclatMapReducer.EclatResult result = runEclat(eclat, List.of(field1, field2, field3), transactionStore);
        assertThat(result.getFrequentItemSets().length, equalTo(2));
        assertThat(result.getFrequentItemSets()[0].getSupport(), equalTo(0.5));
        assertThat(result.getFrequentItemSets()[1].getSupport(), equalTo(0.25));
        assertThat(result.getProfilingInfo().get("unique_items_after_reduce"), equalTo(18L));
        assertThat(result.getProfilingInfo().get("total_transactions_after_reduce"), equalTo(12L));
        assertThat(result.getProfilingInfo().get("total_items_after_reduce"), equalTo(36L));
    }

    public void testPruneToNextMainBranch() throws IOException {
        Field field1 = createKeywordFieldTestInstance("keyword1", 0);
        Field field2 = createKeywordFieldTestInstance("keyword2", 1);
        Field field3 = createKeywordFieldTestInstance("keyword3", 2);

        // create 3 fields that "follow" field 2
        Field field2a = createKeywordFieldTestInstance("keyword2a", 3);
        Field field2b = createKeywordFieldTestInstance("keyword2b", 4);
        Field field2c = createKeywordFieldTestInstance("keyword2c", 5);

        EclatMapReducer eclat = new EclatMapReducer(getTestName(), 0.1, 2, 10, true);

        HashBasedTransactionStore transactionStore = eclat.mapInit(mockBigArrays());
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-a"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-A"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-a"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-B"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-a"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-C"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-a"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-D"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-a"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-E"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-b"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-F"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-b"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-G"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-c"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-H"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-d"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-I"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-b"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-J"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-f"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-K"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-a"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-L"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1")
                )
            ),
            transactionStore
        );

        EclatMapReducer.EclatResult result = runEclat(eclat, List.of(field1, field2, field3, field2a, field2b, field2c), transactionStore);
        assertThat(result.getFrequentItemSets().length, equalTo(3));
        assertThat(result.getFrequentItemSets()[0].getSupport(), equalTo(1.0));
        assertThat(result.getFrequentItemSets()[1].getSupport(), equalTo(0.5));
        assertThat(result.getFrequentItemSets()[2].getSupport(), equalTo(0.25));
        assertThat(result.getProfilingInfo().get("unique_items_after_reduce"), equalTo(21L));
        assertThat(result.getProfilingInfo().get("total_transactions_after_reduce"), equalTo(12L));
        assertThat(result.getProfilingInfo().get("total_items_after_reduce"), equalTo(72L));
        assertThat(result.getProfilingInfo().get("item_sets_checked_eclat"), equalTo(47L));
    }

    public void testPruneToNextMainBranchAfterMinCountPrune() throws IOException {
        Field field1 = createKeywordFieldTestInstance("keyword1", 0);
        Field field2 = createKeywordFieldTestInstance("keyword2", 1);
        Field field3 = createKeywordFieldTestInstance("keyword3", 2);

        // create 3 fields that "follow" field 2
        Field field2a = createKeywordFieldTestInstance("keyword2a", 3);
        Field field2b = createKeywordFieldTestInstance("keyword2b", 4);
        Field field2c = createKeywordFieldTestInstance("keyword2c", 5);

        // create another field to enforce min count pruning
        Field field4 = createKeywordFieldTestInstance("keyword4", 6);
        Field field4a = createKeywordFieldTestInstance("keyword4a", 7);

        EclatMapReducer eclat = new EclatMapReducer(getTestName(), 0.1, 2, 10, true);

        HashBasedTransactionStore transactionStore = eclat.mapInit(mockBigArrays());
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-a"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-A"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1"),
                    tuple(field4, "f4-1"),
                    tuple(field4a, "f4a-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-a"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-B"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1"),
                    tuple(field4, "f4-2"),
                    tuple(field4a, "f4a-2")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-a"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-C"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1"),
                    tuple(field4, "f4-3"),
                    tuple(field4a, "f4a-3")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-a"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-D"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1"),
                    tuple(field4, "f4-4"),
                    tuple(field4a, "f4a-4")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-a"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-E"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1"),
                    tuple(field4, "f4-5"),
                    tuple(field4a, "f4a-5")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-b"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-F"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1"),
                    tuple(field4, "f4-1"),
                    tuple(field4a, "f4a-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-b"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-G"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1"),
                    tuple(field4, "f4-1"),
                    tuple(field4a, "f4a-1")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-c"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-H"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1"),
                    tuple(field4, "f4-6"),
                    tuple(field4a, "f4a-6")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-d"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-I"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1"),
                    tuple(field4, "f4-7"),
                    tuple(field4a, "f4a-7")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-b"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-J"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1"),
                    tuple(field4, "f4-8"),
                    tuple(field4a, "f4a-8")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-f"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-K"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1"),
                    tuple(field4, "f4-3"),
                    tuple(field4a, "f4a-3")
                )
            ),
            transactionStore
        );
        eclat.map(
            mockOneDocument(
                List.of(
                    tuple(field1, "f1-a"),
                    tuple(field2, "f2-1"),
                    tuple(field3, "f3-L"),
                    tuple(field2a, "f2a-1"),
                    tuple(field2b, "f2b-1"),
                    tuple(field2c, "f2c-1"),
                    tuple(field4, "f4-10"),
                    tuple(field4a, "f4a-10")
                )
            ),
            transactionStore
        );

        EclatMapReducer.EclatResult result = runEclat(
            eclat,
            List.of(field1, field2, field3, field2a, field2b, field2c, field4, field4a),
            transactionStore
        );

        assertThat(result.getFrequentItemSets().length, equalTo(6));
        assertThat(result.getFrequentItemSets()[0].getSupport(), equalTo(1.0));
        assertThat(result.getFrequentItemSets()[1].getSupport(), equalTo(0.5));
        assertThat(result.getFrequentItemSets()[2].getSupport(), equalTo(0.25));
        assertThat(result.getProfilingInfo().get("unique_items_after_reduce"), equalTo(39L));
        assertThat(result.getProfilingInfo().get("unique_items_after_prune"), equalTo(10L));
        assertThat(result.getProfilingInfo().get("total_transactions_after_reduce"), equalTo(12L));
        assertThat(result.getProfilingInfo().get("total_items_after_reduce"), equalTo(96L));
        assertThat(result.getProfilingInfo().get("total_items_after_prune"), equalTo(96L));

        // the number can vary depending on order, so we can only check a range, which is still much lower than without
        // that optimization
        assertThat((Long) result.getProfilingInfo().get("item_sets_checked_eclat"), greaterThanOrEqualTo(294L));
        assertThat((Long) result.getProfilingInfo().get("item_sets_checked_eclat"), lessThan(310L));
    }

    private static BigArrays mockBigArrays() {
        return new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

    private static Tuple<Field, List<Object>> mockOneField(Field field, String... fieldValues) {
        return tuple(field, Arrays.stream(fieldValues).map(v -> new BytesRef(v)).collect(Collectors.toList()));
    }

    private static Stream<Tuple<Field, List<Object>>> mockOneDocument(List<Tuple<Field, String>> fieldsAndValues) {
        return fieldsAndValues.stream().map(fieldAndValue -> mockOneField(fieldAndValue.v1(), fieldAndValue.v2()));
    }

    private static EclatMapReducer.EclatResult runEclat(
        EclatMapReducer eclat,
        List<Field> fields,
        HashBasedTransactionStore... transactionStores
    ) throws IOException {
        HashBasedTransactionStore transactionStoreForReduce = eclat.reduceInit(mockBigArrays());

        for (HashBasedTransactionStore transactionStore : transactionStores) {
            ImmutableTransactionStore transactionStoreAfterFinalizing = eclat.mapFinalize(transactionStore, null);
            List<ImmutableTransactionStore> allPartitions = List.of(transactionStoreAfterFinalizing);
            eclat.reduce(allPartitions.stream(), transactionStoreForReduce, doNotCancelSupplier);
        }
        return eclat.reduceFinalize(transactionStoreForReduce, fields, doNotCancelSupplier);
    }
}
