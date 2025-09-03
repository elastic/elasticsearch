/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

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
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.model.TestModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.generateRandomChunkingSettings;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomChunkedInferenceEmbeddingByte;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomChunkedInferenceEmbeddingFloat;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.randomChunkedInferenceEmbeddingSparse;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextFieldTests.semanticTextFieldFromChunkedInferenceResults;
import static org.hamcrest.Matchers.equalTo;

public class SemanticInferenceMetadataFieldsRecoveryTests extends EngineTestCase {
    private final Model model1;
    private final Model model2;
    private final ChunkingSettings chunkingSettings;
    private final boolean useSynthetic;
    private final boolean useIncludesExcludes;

    public SemanticInferenceMetadataFieldsRecoveryTests(boolean useSynthetic, boolean useIncludesExcludes) {
        this.model1 = TestModel.createRandomInstance(TaskType.TEXT_EMBEDDING, List.of(SimilarityMeasure.DOT_PRODUCT));
        this.model2 = TestModel.createRandomInstance(TaskType.SPARSE_EMBEDDING);
        this.chunkingSettings = generateRandomChunkingSettings();
        this.useSynthetic = useSynthetic;
        this.useIncludesExcludes = useIncludesExcludes;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(new Object[] { false, false }, new Object[] { false, true }, new Object[] { true, false });
    }

    @Override
    protected List<MapperPlugin> extraMappers() {
        return List.of(new InferencePlugin(Settings.EMPTY));
    }

    @Override
    protected Settings indexSettings() {
        var builder = Settings.builder().put(super.indexSettings());
        if (useSynthetic) {
            builder.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC.name());
            builder.put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true);
        }
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

            builder.startObject("semantic_1");
            builder.field("type", "semantic_text");
            builder.field("inference_id", model1.getInferenceEntityId());
            builder.startObject("model_settings");
            builder.field("task_type", model1.getTaskType().name());
            builder.field("dimensions", model1.getServiceSettings().dimensions());
            builder.field("similarity", model1.getServiceSettings().similarity().name());
            builder.field("element_type", model1.getServiceSettings().elementType().name());
            builder.field("service", model1.getConfigurations().getService());
            builder.endObject();
            if (chunkingSettings != null) {
                builder.field("chunking_settings");
                chunkingSettings.toXContent(builder, null);
            }
            builder.endObject();

            builder.startObject("semantic_2");
            builder.field("type", "semantic_text");
            builder.field("inference_id", model2.getInferenceEntityId());
            builder.startObject("model_settings");
            builder.field("task_type", model2.getTaskType().name());
            builder.field("service", model2.getConfigurations().getService());
            builder.endObject();
            if (chunkingSettings != null) {
                builder.field("chunking_settings");
                chunkingSettings.toXContent(builder, null);
            }
            builder.endObject();

            builder.endObject();
            builder.endObject();
            return BytesReference.bytes(builder).utf8ToString();
        } catch (IOException exc) {
            throw new RuntimeException(exc);
        }
    }

    public void testSnapshotRecovery() throws IOException {
        List<Translog.Index> expectedOperations = new ArrayList<>();
        int size = randomIntBetween(10, 50);
        for (int i = 0; i < size; i++) {
            var source = randomSource();
            var sourceToParse = new SourceToParse(Integer.toString(i), source, XContentType.JSON, null);
            var doc = mapperService.documentMapper().parse(sourceToParse);
            assertNull(doc.dynamicMappingsUpdate());
            if (useSynthetic) {
                assertNull(doc.rootDoc().getField(SourceFieldMapper.RECOVERY_SOURCE_NAME));
                assertNotNull(doc.rootDoc().getField(SourceFieldMapper.RECOVERY_SOURCE_SIZE_NAME));
            } else {
                if (useIncludesExcludes) {
                    assertNotNull(doc.rootDoc().getField(SourceFieldMapper.RECOVERY_SOURCE_NAME));
                    var originalSource = new BytesArray(doc.rootDoc().getField(SourceFieldMapper.RECOVERY_SOURCE_NAME).binaryValue());
                    var map = XContentHelper.convertToMap(originalSource, false, XContentType.JSON);
                    assertThat(map.v2().size(), equalTo(1));
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

    private BytesReference randomSource() throws IOException {
        var builder = JsonXContent.contentBuilder().startObject();
        builder.field("field", randomAlphaOfLengthBetween(10, 30));
        if (rarely()) {
            return BytesReference.bytes(builder.endObject());
        }
        SemanticTextFieldMapperTests.addSemanticTextInferenceResults(
            false,
            builder,
            List.of(
                randomSemanticText(false, "semantic_2", model2, chunkingSettings, randomInputs(), XContentType.JSON),
                randomSemanticText(false, "semantic_1", model1, chunkingSettings, randomInputs(), XContentType.JSON)
            )
        );
        builder.endObject();
        return BytesReference.bytes(builder);
    }

    private static SemanticTextField randomSemanticText(
        boolean useLegacyFormat,
        String fieldName,
        Model model,
        ChunkingSettings chunkingSettings,
        List<String> inputs,
        XContentType contentType
    ) throws IOException {
        ChunkedInference results = switch (model.getTaskType()) {
            case TEXT_EMBEDDING -> switch (model.getServiceSettings().elementType()) {
                case FLOAT -> randomChunkedInferenceEmbeddingFloat(model, inputs);
                case BYTE, BIT -> randomChunkedInferenceEmbeddingByte(model, inputs);
            };
            case SPARSE_EMBEDDING -> randomChunkedInferenceEmbeddingSparse(inputs, false);
            default -> throw new AssertionError("invalid task type: " + model.getTaskType().name());
        };
        return semanticTextFieldFromChunkedInferenceResults(
            useLegacyFormat,
            fieldName,
            model,
            chunkingSettings,
            inputs,
            results,
            contentType
        );
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
