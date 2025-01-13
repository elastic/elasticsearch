/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.LuceneChangesSnapshot;
import org.elasticsearch.index.engine.LuceneSyntheticSourceChangesSnapshot;
import org.elasticsearch.index.engine.SearchBasedChangesSnapshot;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.model.TestModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomSemanticText;
import static org.hamcrest.Matchers.equalTo;

public class SemanticInferenceMetadataFieldsRecoveryTests extends EngineTestCase {
    private final XContentType xContentType;
    private final Model model1;
    private final Model model2;
    private final boolean useSynthetic;

    public SemanticInferenceMetadataFieldsRecoveryTests(boolean useSynthetic) {
        this.xContentType = randomFrom(XContentType.JSON);
        this.model1 = randomModel(TaskType.TEXT_EMBEDDING);
        this.model2 = randomModel(TaskType.SPARSE_EMBEDDING);
        this.useSynthetic = useSynthetic;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(new Object[] { false }, new Object[] { true });
    }

    @Override
    protected List<MapperPlugin> extraMappers() {
        return List.of(new InferencePlugin(Settings.EMPTY));
    }

    @Override
    protected Settings indexSettings() {
        var builder = Settings.builder()
            .put(super.indexSettings())
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), false);
        if (useSynthetic) {
            builder.put(SourceFieldMapper.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.name());
            builder.put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true);
        }
        return builder.build();
    }

    @Override
    protected String defaultMapping() {
        return String.format(
            Locale.ROOT,
            """
                {
                    "dynamic": false,
                    "properties": {
                        "field": {
                          "type": "keyword"
                        },
                        "semantic_1": {
                            "type": "semantic_text",
                            "inference_id": "%s",
                            "model_settings": {
                              "task_type": "text_embedding",
                              "dimensions": %d,
                              "similarity": "%s",
                              "element_type": "%s"
                            }
                        },
                        "semantic_2": {
                            "type": "semantic_text",
                            "inference_id": "%s",
                            "model_settings": {
                              "task_type": "sparse_embedding"
                            }
                        }
                    }
                }
                """,
            model1.getInferenceEntityId(),
            model1.getServiceSettings().dimensions(),
            model1.getServiceSettings().similarity().name(),
            model1.getServiceSettings().elementType().name(),
            model2.getInferenceEntityId()
        );
    }

    public void testSnapshotRecovery() throws IOException {
        List<Engine.Index> expectedOperations = new ArrayList<>();
        int size = randomIntBetween(10, 50);
        for (int i = 0; i < size; i++) {
            var source = randomSource();
            var sourceToParse = new SourceToParse(Integer.toString(i), source, xContentType, null);
            var doc = mapperService.documentMapper().parse(sourceToParse);
            assertNull(doc.dynamicMappingsUpdate());
            var op = indexForDoc(doc);
            expectedOperations.add(op);
            engine.index(op);
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
            for (int i = 0; i < size; i++) {
                var op = snapshot.next();
                assertThat(op.opType(), equalTo(Translog.Operation.Type.INDEX));
                Translog.Index indexOp = (Translog.Index) op;
                assertThat(indexOp.id(), equalTo(expectedOperations.get(i).id()));
                assertThat(indexOp.routing(), equalTo(expectedOperations.get(i).routing()));
                assertToXContentEquivalent(indexOp.source(), expectedOperations.get(i).source(), xContentType);
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
        if (useSynthetic) {
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

    private static Model randomModel(TaskType taskType) {
        var dimensions = taskType == TaskType.TEXT_EMBEDDING ? randomIntBetween(2, 64) : null;
        var similarity = taskType == TaskType.TEXT_EMBEDDING ? randomFrom(SimilarityMeasure.values()) : null;
        var elementType = taskType == TaskType.TEXT_EMBEDDING ? DenseVectorFieldMapper.ElementType.BYTE : null;
        return new TestModel(
            randomAlphaOfLength(4),
            taskType,
            randomAlphaOfLength(10),
            new TestModel.TestServiceSettings(randomAlphaOfLength(4), dimensions, similarity, elementType),
            new TestModel.TestTaskSettings(randomInt(3)),
            new TestModel.TestSecretSettings(randomAlphaOfLength(4))
        );
    }

    private BytesReference randomSource() throws IOException {
        var builder = XContentBuilder.builder(xContentType.xContent()).startObject();
        builder.field("field", randomAlphaOfLengthBetween(10, 30));
        if (rarely()) {
            return BytesReference.bytes(builder.endObject());
        }
        SemanticTextFieldMapperTests.addSemanticTextInferenceResults(
            false,
            builder,
            List.of(
                randomSemanticText(false, "semantic_2", model2, randomInputs(), xContentType),
                randomSemanticText(false, "semantic_1", model1, randomInputs(), xContentType)
            )
        );
        builder.endObject();
        return BytesReference.bytes(builder);
    }

    private static List<String> randomInputs() {
        int size = randomIntBetween(1, 5);
        List<String> resp = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            resp.add(randomAlphaOfLengthBetween(10, 50));
        }
        return resp;
    }
}
