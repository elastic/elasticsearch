/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.LuceneChangesSnapshot;
import org.elasticsearch.index.engine.LuceneSyntheticSourceChangesSnapshot;
import org.elasticsearch.index.engine.SearchBasedChangesSnapshot;
import org.elasticsearch.index.engine.TranslogOperationAsserter;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.IndexSettings.SYNTHETIC_VECTORS;
import static org.hamcrest.Matchers.equalTo;

public class SyntheticVectorFieldsRecoveryTests extends EngineTestCase {
    private final boolean useSynthetic;
    private final boolean useSyntheticRecovery;
    private final boolean useIncludesExcludes;

    public SyntheticVectorFieldsRecoveryTests(boolean useSynthetic, boolean useSyntheticRecovery, boolean useIncludesExcludes) {
        this.useSynthetic = useSynthetic;
        this.useSyntheticRecovery = useSyntheticRecovery;
        this.useIncludesExcludes = useIncludesExcludes;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(
            new Object[] { false, false, false },
            new Object[] { false, false, true },
            new Object[] { true, false, false },
            new Object[] { true, true, false }
        );
    }

    @Override
    protected Settings indexSettings() {
        var builder = Settings.builder().put(super.indexSettings());
        if (useSynthetic) {
            builder.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.name());
            builder.put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), useSyntheticRecovery);
        }
        builder.put(IndexSettings.INDEX_MAPPING_SOURCE_SYNTHETIC_VECTORS_SETTING.getKey(), true);
        return builder.build();
    }

    @Override
    protected String defaultMapping() {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder().startObject();
            if (useIncludesExcludes) {
                builder.startObject(SourceFieldMapper.NAME).array("excludes", "field").endObject();
            }
            builder.field("dynamic", false);
            builder.startObject("properties");

            builder.startObject("field");
            builder.field("type", "keyword");
            builder.endObject();

            builder.startObject("emb");
            builder.field("type", "dense_vector");
            builder.field("dims", 10);
            builder.field("similarity", "l2_norm");
            builder.endObject();

            builder.startObject("nested");
            builder.field("type", "nested");
            builder.startObject("properties");
            builder.startObject("emb");
            builder.field("type", "dense_vector");
            builder.field("dims", 10);
            builder.field("similarity", "l2_norm");
            builder.endObject();
            builder.endObject();
            builder.endObject();

            builder.endObject();
            builder.endObject();
            return BytesReference.bytes(builder).utf8ToString();
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    public void testSnapshotRecovery() throws IOException {
        assumeTrue("feature flag must be enabled for synthetic vectors", SYNTHETIC_VECTORS);
        List<Translog.Index> expectedOperations = new ArrayList<>();
        int size = randomIntBetween(10, 50);
        for (int i = 0; i < size; i++) {
            var source = randomSource();
            var sourceToParse = new SourceToParse(Integer.toString(i), source, XContentType.JSON, null);
            var doc = mapperService.documentMapper().parse(sourceToParse);
            assertNull(doc.dynamicMappingsUpdate());
            if (useSynthetic) {
                if (useSyntheticRecovery) {
                    assertNull(doc.rootDoc().getField(SourceFieldMapper.RECOVERY_SOURCE_NAME));
                    assertNotNull(doc.rootDoc().getField(SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME));
                } else {
                    assertNotNull(doc.rootDoc().getField(SourceFieldMapper.RECOVERY_SOURCE_NAME));
                    assertNull(doc.rootDoc().getField(SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME));
                }
            } else {
                if (useIncludesExcludes) {
                    assertNotNull(doc.rootDoc().getField(SourceFieldMapper.RECOVERY_SOURCE_NAME));
                    var originalSource = new BytesArray(doc.rootDoc().getField(SourceFieldMapper.RECOVERY_SOURCE_NAME).binaryValue());
                    var map = XContentHelper.convertToMap(originalSource, false, XContentType.JSON);
                    assertThat(map.v2().size(), Matchers.anyOf(equalTo(1), equalTo(2)));
                    assertNull(map.v2().remove(InferenceMetadataFieldsMapper.NAME));
                } else {
                    assertNull(doc.rootDoc().getField(SourceFieldMapper.RECOVERY_SOURCE_NAME));
                }
            }
            var op = indexForDoc(doc);
            var result = engine.index(op);
            expectedOperations.add(
                new Translog.Index(
                    result.getId(),
                    result.getSeqNo(),
                    result.getTerm(),
                    result.getVersion(),
                    op.source(),
                    op.routing(),
                    op.getAutoGeneratedIdTimestamp()
                )
            );

            if (frequently()) {
                engine.flush();
            }
        }
        engine.flush();

        var searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
        try (
            var snapshot = newRandomSnapshot(
                engine.config().getMapperService(),
                searcher,
                SearchBasedChangesSnapshot.DEFAULT_BATCH_SIZE,
                0,
                size - 1,
                true,
                randomBoolean(),
                randomBoolean(),
                IndexVersion.current()
            )
        ) {
            var asserter = TranslogOperationAsserter.withEngineConfig(engine.config());
            for (int i = 0; i < size; i++) {
                var op = snapshot.next();
                assertThat(op.opType(), equalTo(Translog.Operation.Type.INDEX));
                Translog.Index indexOp = (Translog.Index) op;
                asserter.assertSameIndexOperation(indexOp, expectedOperations.get(i));
            }
            assertNull(snapshot.next());
        }
    }

    private Translog.Snapshot newRandomSnapshot(
        MapperService mapperService,
        Engine.Searcher engineSearcher,
        int searchBatchSize,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats,
        IndexVersion indexVersionCreated
    ) throws IOException {
        if (useSyntheticRecovery) {
            return new LuceneSyntheticSourceChangesSnapshot(
                mapperService,
                engineSearcher,
                searchBatchSize,
                randomLongBetween(0, ByteSizeValue.ofBytes(Integer.MAX_VALUE).getBytes()),
                fromSeqNo,
                toSeqNo,
                requiredFullRange,
                accessStats,
                indexVersionCreated
            );
        } else {
            return new LuceneChangesSnapshot(
                mapperService,
                engineSearcher,
                searchBatchSize,
                fromSeqNo,
                toSeqNo,
                requiredFullRange,
                singleConsumer,
                accessStats,
                indexVersionCreated
            );
        }
    }

    private BytesReference randomSource() throws IOException {
        var builder = JsonXContent.contentBuilder().startObject();
        builder.field("field", randomAlphaOfLengthBetween(10, 30));
        if (rarely()) {
            return BytesReference.bytes(builder.endObject());
        }

        if (usually()) {
            builder.field("emb", randomVector());
        }

        if (randomBoolean()) {
            int numNested = randomIntBetween(0, 6);
            builder.startArray("nested");
            for (int i = 0; i < numNested; i++) {
                builder.startObject();
                if (randomBoolean()) {
                    builder.field("paragraph_id", i);
                }
                if (randomBoolean()) {
                    builder.field("emb", randomVector());
                }
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        return BytesReference.bytes(builder);
    }

    private static float[] randomVector() {
        float[] vector = new float[10];
        for (int i = 0; i < 10; i++) {
            vector[i] = randomByte();
        }
        return vector;
    }
}
