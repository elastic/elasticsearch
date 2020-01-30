/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.search.rescore.RescorerBuilder;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.SingleValueInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.WarningInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class InferenceRescorerBuilder extends RescorerBuilder<InferenceRescorerBuilder> {

    public static final String NAME = "ml_rescore";

    private static final Logger logger = LogManager.getLogger(InferenceRescorerBuilder.class);

    // TODO - window size??
    // combine scores
    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");
    public static final ParseField FIELD_MAPPINGS = new ParseField("field_mappings");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<InferenceRescorerBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> new InferenceRescorerBuilder((String) args[0], (List<InferenceConfig>) args[1], (Map<String, String>) args[2]));

    static {
        PARSER.declareString(constructorArg(), MODEL_ID);
        PARSER.declareNamedObjects(optionalConstructorArg(), (p, c, n) -> p.namedObject(InferenceConfig.class, n, c),  INFERENCE_CONFIG);
        PARSER.declareField(optionalConstructorArg(), (p, c) -> p.mapStrings(), FIELD_MAPPINGS, ObjectParser.ValueType.OBJECT);
    }

    public static InferenceRescorerBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String modelId;
    private final InferenceConfig inferenceConfig;
    private final Map<String, String> fieldMap;

    private LocalModel model;
    private Supplier<LocalModel> modelSupplier;

    private InferenceRescorerBuilder(String modelId, @Nullable List<InferenceConfig> config, @Nullable Map<String, String> fieldMap) {
        this.modelId = modelId;
        if (config != null) {
            assert config.size() == 1;
            this.inferenceConfig = config.get(0);
        } else {
            this.inferenceConfig = null;
        }
        this.fieldMap = fieldMap;
    }

    InferenceRescorerBuilder(String modelId, @Nullable InferenceConfig config, @Nullable Map<String, String> fieldMap) {
        this.modelId = modelId;
        this.inferenceConfig = config;
        this.fieldMap = fieldMap;
    }

    InferenceRescorerBuilder(String modelId, @Nullable InferenceConfig config, @Nullable Map<String, String> fieldMap,
                             Supplier<LocalModel> modelSupplier) {
        this(modelId, config, fieldMap);
        this.modelSupplier = modelSupplier;
    }

    InferenceRescorerBuilder(String modelId, @Nullable InferenceConfig config, @Nullable Map<String, String> fieldMap,
                             LocalModel model) {
        this(modelId, config, fieldMap);
        this.model = Objects.requireNonNull(model);
    }

    public InferenceRescorerBuilder(StreamInput in) throws IOException {
        super(in);
        modelId = in.readString();
        inferenceConfig = in.readOptionalNamedWriteable(InferenceConfig.class);
        boolean readMap = in.readBoolean();
        if (readMap) {
            fieldMap = in.readMap(StreamInput::readString, StreamInput::readString);
        } else {
            fieldMap = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (modelSupplier != null) {
            throw new IllegalStateException("can't serialize model supplier. Missing a rewriteAndFetch?");
        }

        out.writeString(modelId);
        out.writeOptionalNamedWriteable(inferenceConfig);
        boolean fieldMapPresent = fieldMap != null;
        out.writeBoolean(fieldMapPresent);
        if (fieldMapPresent) {
            out.writeMap(fieldMap, StreamOutput::writeString, StreamOutput::writeString);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID.getPreferredName(), modelId);
        if (inferenceConfig != null) {
            builder.startObject(INFERENCE_CONFIG.getPreferredName());
            builder.field(inferenceConfig.getName(), inferenceConfig);
            builder.endObject();
        }
        if (fieldMap != null) {
            builder.field(FIELD_MAPPINGS.getPreferredName(), fieldMap);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public RescorerBuilder<InferenceRescorerBuilder> rewrite(QueryRewriteContext ctx) {

        assert modelId != null;

        if (model != null) {
            return this;
        } else if (modelSupplier != null) {
            if (modelSupplier.get() == null) {
                return this;
            } else {
                return new InferenceRescorerBuilder(modelId, inferenceConfig, fieldMap, modelSupplier.get());
            }
        } else {
            SetOnce<LocalModel> modelHolder = new SetOnce<>();

            ctx.registerAsyncAction(((client, actionListener) -> {
                TrainedModelProvider modelProvider = new TrainedModelProvider(client, ctx.getXContentRegistry());
                modelProvider.getTrainedModel(modelId, true, ActionListener.wrap(
                        trainedModel -> {
                            LocalModel model = new LocalModel(modelId,
                                    trainedModel.ensureParsedDefinition(ctx.getXContentRegistry()).getModelDefinition(),
                                    trainedModel.getInput());
                            modelHolder.set(model);
                            actionListener.onResponse(null);
                        },
                        actionListener::onFailure
                ));
            }));

            return new InferenceRescorerBuilder(modelId, inferenceConfig, fieldMap, modelHolder::get);
        }
    }

    @Override
    protected RescoreContext innerBuildContext(int windowSize, QueryShardContext context) {
        LocalModel m = (model != null) ? model : modelSupplier.get();
        assert m != null;
        return new RescoreContext(windowSize, new InferenceRescorer(m, inferenceConfig, fieldMap));
    }

    @Override
    public final int hashCode() {
        return Objects.hash(windowSize, modelId, inferenceConfig, fieldMap);
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        InferenceRescorerBuilder other = (InferenceRescorerBuilder) obj;
        return Objects.equals(windowSize, other.windowSize) &&
                Objects.equals(modelId, other.modelId) &&
                Objects.equals(inferenceConfig, other.inferenceConfig) &&
                Objects.equals(fieldMap, other.fieldMap);
    }


    private static class InferenceRescorer implements Rescorer {

        private final LocalModel model;
        private final InferenceConfig inferenceConfig;
        private final Map<String, String> fieldMap;


        private InferenceRescorer(LocalModel model, InferenceConfig inferenceConfig, Map<String, String> fieldMap) {
            this.model = model;
            this.inferenceConfig = inferenceConfig;
            this.fieldMap = fieldMap;

            assert inferenceConfig instanceof RegressionConfig;
        }

        @Override
        public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {

            // Copy ScoreDoc[] and sort by ascending docID:
            ScoreDoc[] sortedHits = topDocs.scoreDocs.clone();
            Comparator<ScoreDoc> docIdComparator = Comparator.comparingInt(sd -> sd.doc);
            Arrays.sort(sortedHits, docIdComparator);

            // field map is fieldname in doc -> fieldname expected by model
            Set<String> fieldsToRead = new HashSet<>(model.getFieldNames());
            for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
                if (fieldsToRead.contains(entry.getValue())) {
                    // replace the model fieldname with the doc fieldname
                    fieldsToRead.remove(entry.getValue());
                    fieldsToRead.add(entry.getKey());
                }
            }

            List<LeafReaderContext> leaves = searcher.getIndexReader().getContext().leaves();
            Map<String, Object> fields = new HashMap<>();
            for (int i=0; i<sortedHits.length; i++) {
                ScoreDoc scoreDoc = sortedHits[i];

                LeafReaderContext leafContext = leaves.get(ReaderUtil.subIndex(scoreDoc.doc, leaves));

                logger.info("doc id {}", scoreDoc.doc);
                for (String field : fieldsToRead) {
                    SortedNumericDocValues docValuesIter = DocValues.getSortedNumeric(leafContext.reader(), field);
                    SortedNumericDoubleValues doubles = FieldData.sortableLongBitsToDoubles(docValuesIter);
                    if (doubles.advanceExact(scoreDoc.doc)) {
                        double val = doubles.nextValue();
                        logger.warn("got value {} for field {}, doc {}", val, field, scoreDoc.doc);
                        fields.put(fieldMap.getOrDefault(field, field), val);
                    } else if (docValuesIter.docID() == DocIdSetIterator.NO_MORE_DOCS) {
                        logger.warn("No more docs for field {}, doc {}", field, scoreDoc.doc);
                        fields.remove(field);
                    } else {
                        logger.warn("no value for field {}, doc {}", field, scoreDoc.doc);
                        fields.remove(field);
                    }
                }

                InferenceResults infer = model.infer(fields, inferenceConfig);
                if (infer instanceof WarningInferenceResults) {
                    logger.warn("inference error: " + ((WarningInferenceResults) infer).getWarning());
                } else {
                    SingleValueInferenceResults regressionResult = (SingleValueInferenceResults) infer;
                    sortedHits[i] = new ScoreDoc(scoreDoc.doc, regressionResult.value().floatValue());
                }
            }

            return new TopDocs(topDocs.totalHits, sortedHits);
        }

        @Override
        public Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext,
                                   Explanation sourceExplanation) {
            return Explanation.match(1.0, "because");
        }
    }

}
