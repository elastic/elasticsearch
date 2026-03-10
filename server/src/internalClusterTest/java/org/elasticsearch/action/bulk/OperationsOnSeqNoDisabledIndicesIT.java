/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.OCCNotSupportedException;
import org.elasticsearch.index.engine.UpdateNotSupportedException;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class OperationsOnSeqNoDisabledIndicesIT extends ESIntegTestCase {
    public void testBulkWithMixedOperationsAcrossSeqNoDisabledAndEnabledIndices() {
        assumeTrue("Test requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);

        final var numSeqNoDisabled = randomIntBetween(1, 3);
        final var numSeqNoEnabled = randomIntBetween(1, 3);
        final var seqNoDisabledIndices = new HashSet<String>(numSeqNoDisabled);
        final var allIndices = new HashSet<String>();

        for (int i = 0; i < numSeqNoDisabled; i++) {
            var indexName = "seq-no-disabled-" + i;
            createIndex(
                indexName,
                indexSettings(1, 1).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true)
                    .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
                    .build()
            );
            seqNoDisabledIndices.add(indexName);
            allIndices.add(indexName);
        }
        for (int i = 0; i < numSeqNoEnabled; i++) {
            var indexName = "seq-no-enabled-" + i;
            createIndex(indexName, indexSettings(1, 1).build());
            allIndices.add(indexName);
        }
        ensureGreen();

        // Seed documents in all indices
        var seedBulk = client().prepareBulk();
        for (String index : allIndices) {
            int docsPerIndex = randomIntBetween(10, 20);
            for (int i = 0; i < docsPerIndex; i++) {
                seedBulk.add(prepareIndex(index).setSource("value", "original"));
            }
        }
        var seedResponse = seedBulk.get();
        assertNoFailures(seedResponse);

        Map<String, Map<String, DocMetadata>> docsMetadataByIndex = new HashMap<>();
        for (BulkItemResponse item : seedResponse) {
            String index = item.getIndex();
            DocWriteResponse response = item.getResponse();
            docsMetadataByIndex.computeIfAbsent(index, k -> new HashMap<>())
                .put(response.getId(), new DocMetadata(response.getId(), response.getSeqNo(), response.getPrimaryTerm()));
        }

        // Build a mixed bulk request across all indices
        var mixedBulk = client().prepareBulk();
        var expectedFailureTypes = new ArrayList<Class<? extends Exception>>();

        Map<String, Map<String, ExpectedDocContents>> expectedDocContentsByIndex = new HashMap<>();
        for (String index : allIndices) {
            boolean isSeqNoDisabled = seqNoDisabledIndices.contains(index);
            var docsMetadata = docsMetadataByIndex.get(index);

            for (DocMetadata docMetadata : docsMetadata.values()) {
                var id = docMetadata.id();
                final Map<String, ExpectedDocContents> indexDocExpectations = expectedDocContentsByIndex.computeIfAbsent(
                    index,
                    k -> new HashMap<>()
                );
                switch (randomFrom(BulkOperationType.values())) {
                    case INDEX -> {
                        mixedBulk.add(prepareIndex(index).setId(id).setSource("value", "override"));
                        expectedFailureTypes.add(null);
                        indexDocExpectations.put(id, new ExpectedDocContents(id, "override", false));
                    }
                    case INDEX_WITH_OCC -> {
                        long seqNo = isSeqNoDisabled ? randomIntBetween(1, 1000) : docMetadata.seqNo();
                        long primaryTerm = isSeqNoDisabled ? 1 : docMetadata.primaryTerm();
                        mixedBulk.add(
                            prepareIndex(index).setId(id).setSource("value", "occ-indexed").setIfSeqNo(seqNo).setIfPrimaryTerm(primaryTerm)
                        );
                        if (isSeqNoDisabled) {
                            indexDocExpectations.put(id, new ExpectedDocContents(id, "original", false));
                            expectedFailureTypes.add(OCCNotSupportedException.class);
                        } else {
                            indexDocExpectations.put(id, new ExpectedDocContents(id, "occ-indexed", false));
                            expectedFailureTypes.add(null);
                        }
                    }
                    case DELETE -> {
                        mixedBulk.add(client().prepareDelete(index, id));
                        indexDocExpectations.put(id, new ExpectedDocContents(id, null, true));
                        expectedFailureTypes.add(null);
                    }
                    case DELETE_WITH_OCC -> {
                        long seqNo = isSeqNoDisabled ? randomIntBetween(1, 1000) : docMetadata.seqNo();
                        long primaryTerm = isSeqNoDisabled ? 1 : docMetadata.primaryTerm();

                        mixedBulk.add(client().prepareDelete(index, id).setIfSeqNo(seqNo).setIfPrimaryTerm(primaryTerm));
                        if (isSeqNoDisabled) {
                            indexDocExpectations.put(id, new ExpectedDocContents(id, "original", false));
                            expectedFailureTypes.add(OCCNotSupportedException.class);
                        } else {
                            indexDocExpectations.put(id, new ExpectedDocContents(id, null, true));
                            expectedFailureTypes.add(null);
                        }
                    }
                    case UPDATE -> {
                        mixedBulk.add(client().prepareUpdate(index, id).setDoc("value", "updated"));
                        if (isSeqNoDisabled) {
                            indexDocExpectations.put(id, new ExpectedDocContents(id, "original", false));
                            expectedFailureTypes.add(UpdateNotSupportedException.class);
                        } else {
                            indexDocExpectations.put(id, new ExpectedDocContents(id, "updated", false));
                            expectedFailureTypes.add(null);
                        }
                    }
                    case SKIP -> indexDocExpectations.put(id, new ExpectedDocContents(id, "original", false));
                }
            }
        }

        var mixedResponse = mixedBulk.get();
        for (int i = 0; i < mixedResponse.getItems().length; i++) {
            var item = mixedResponse.getItems()[i];
            var expectedFailure = expectedFailureTypes.get(i);
            if (expectedFailure != null) {
                assertThat("item [" + i + "] targeting [" + item.getIndex() + "] should have failed", item.isFailed(), is(true));
                assertThat(
                    "item [" + i + "] targeting [" + item.getIndex() + "] failure type",
                    item.getFailure().getCause(),
                    is(instanceOf(expectedFailure))
                );
            } else {
                assertFalse("item [" + i + "] targeting [" + item.getIndex() + "]: " + item.getFailureMessage(), item.isFailed());
            }
        }

        // Verify document contents and deletions across all indices
        for (String index : allIndices) {
            indicesAdmin().prepareRefresh(index).get();
            var expectedDocsContent = expectedDocContentsByIndex.get(index);
            assertThat(expectedDocsContent, is(notNullValue()));

            long expectedSurvivingDocs = expectedDocsContent.values().stream().filter(e -> e.deleted() == false).count();
            var expectedDeletedIds = new HashSet<String>();
            for (var entry : expectedDocsContent.entrySet()) {
                if (entry.getValue().deleted()) {
                    expectedDeletedIds.add(entry.getKey());
                }
            }

            assertResponse(prepareSearch(index).setQuery(new MatchAllQueryBuilder()).setSize(1000), searchResponse -> {
                assertThat(
                    "index [" + index + "] should have [" + expectedSurvivingDocs + "] surviving docs",
                    searchResponse.getHits().getTotalHits().value(),
                    equalTo(expectedSurvivingDocs)
                );

                for (var searchHit : searchResponse.getHits().getHits()) {
                    String docId = searchHit.getId();
                    assertFalse("doc [" + docId + "] in [" + index + "] should have been deleted", expectedDeletedIds.contains(docId));
                    ExpectedDocContents expected = expectedDocsContent.get(docId);
                    assertThat(expected, is(notNullValue()));
                    assertThat(expected.deleted(), is(false));
                    String docValue = (String) searchHit.getSourceAsMap().get("value");
                    assertThat("doc [" + docId + "] in [" + index + "] has unexpected value", docValue, is(equalTo(expected.value())));
                }
            });
        }
    }

    enum BulkOperationType {
        INDEX,
        INDEX_WITH_OCC,
        DELETE,
        DELETE_WITH_OCC,
        UPDATE,
        SKIP
    }

    record DocMetadata(String id, long seqNo, long primaryTerm) {}

    record ExpectedDocContents(String id, @Nullable String value, boolean deleted) {
        ExpectedDocContents {
            assert value != null ^ deleted : "A document can only be deleted or indexed, not both";
        }
    }

    public void testSingleUpdateOnSeqNoDisabledIndexIsRejected() {
        assumeTrue("Test requires disable_sequence_numbers feature flag", IndexSettings.DISABLE_SEQUENCE_NUMBERS_FEATURE_FLAG);
        createIndex(
            "test",
            indexSettings(1, 0).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true)
                .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
                .build()
        );
        ensureGreen("test");

        var indexResult = prepareIndex("test").setId("1").setSource("value", "original").get();
        assertThat(indexResult.getResult(), equalTo(DocWriteResponse.Result.CREATED));

        var exception = expectThrows(
            UpdateNotSupportedException.class,
            () -> client().prepareUpdate("test", "1").setDoc("value", "updated").get()
        );
        assertThat(exception.getMessage(), containsString("Updates are not supported on indices with sequence numbers disabled"));

        // Verify the document was not updated
        var getResponse = client().prepareGet("test", "1").get();
        assertTrue(getResponse.isExists());
        assertThat(getResponse.getSource().get("value"), equalTo("original"));
    }
}
