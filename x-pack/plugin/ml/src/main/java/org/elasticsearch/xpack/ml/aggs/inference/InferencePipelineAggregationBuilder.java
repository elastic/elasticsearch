/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.inference;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ResultsFieldUpdate;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.ml.inference.loadingservice.LocalModel;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xpack.ml.utils.SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable;

public class InferencePipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<InferencePipelineAggregationBuilder> {

    public static final String NAME = "inference";

    public static final ParseField MODEL_ID = new ParseField("model_id");
    private static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");

    static final String AGGREGATIONS_RESULTS_FIELD = "value";

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<InferencePipelineAggregationBuilder, ParserSupplement> PARSER =
        new ConstructingObjectParser<>(NAME, false,
        (args, context) -> new InferencePipelineAggregationBuilder(context.name, context.modelLoadingService,
            context.licenseState, context.settings, (Map<String, String>) args[0])
    );

    static {
        PARSER.declareObject(constructorArg(), (p, c) -> p.mapStrings(), BUCKETS_PATH_FIELD);
        PARSER.declareString(InferencePipelineAggregationBuilder::setModelId, MODEL_ID);
        PARSER.declareNamedObject(InferencePipelineAggregationBuilder::setInferenceConfig,
            (p, c, n) -> p.namedObject(InferenceConfigUpdate.class, n, c), INFERENCE_CONFIG);
    }

    public static SearchPlugin.PipelineAggregationSpec buildSpec(SetOnce<ModelLoadingService> modelLoadingService,
                                                                 XPackLicenseState xPackLicenseState, Settings settings) {
        SearchPlugin.PipelineAggregationSpec spec = new SearchPlugin.PipelineAggregationSpec(InferencePipelineAggregationBuilder.NAME,
            in -> new InferencePipelineAggregationBuilder(in, xPackLicenseState, settings, modelLoadingService),
            (ContextParser<String, ? extends PipelineAggregationBuilder>)
                (parser, name) -> InferencePipelineAggregationBuilder.parse(modelLoadingService, xPackLicenseState, settings, name, parser)
        );
        spec.addResultReader(InternalInferenceAggregation::new);
        return spec;
    }

    private final Map<String, String> bucketPathMap;
    private String modelId;
    private InferenceConfigUpdate inferenceConfig;
    private final XPackLicenseState licenseState;
    private final Settings settings;
    private final SetOnce<ModelLoadingService> modelLoadingService;
    /**
     * The model. Set to a non-null value during the rewrite phase.
     */
    private final Supplier<LocalModel> model;

    private static class ParserSupplement {
        final XPackLicenseState licenseState;
        final Settings settings;
        final SetOnce<ModelLoadingService> modelLoadingService;
        final String name;

        ParserSupplement(String name, XPackLicenseState licenseState, Settings settings, SetOnce<ModelLoadingService> modelLoadingService) {
            this.name = name;
            this.licenseState = licenseState;
            this.settings = settings;
            this.modelLoadingService = modelLoadingService;
        }
    }
    public static InferencePipelineAggregationBuilder parse(SetOnce<ModelLoadingService> modelLoadingService,
                                                            XPackLicenseState licenseState,
                                                            Settings settings,
                                                            String pipelineAggregatorName,
                                                            XContentParser parser) {
        return PARSER.apply(parser, new ParserSupplement(pipelineAggregatorName, licenseState, settings, modelLoadingService));
    }

    public InferencePipelineAggregationBuilder(String name,
                                               SetOnce<ModelLoadingService> modelLoadingService,
                                               XPackLicenseState licenseState,
                                               Settings settings,
                                               Map<String, String> bucketsPath) {
        super(name, NAME, new TreeMap<>(bucketsPath).values().toArray(new String[] {}));
        this.modelLoadingService = modelLoadingService;
        this.bucketPathMap = bucketsPath;
        this.model = null;
        this.licenseState = licenseState;
        this.settings = settings;
    }

    public InferencePipelineAggregationBuilder(StreamInput in,
                                               XPackLicenseState licenseState,
                                               Settings settings,
                                               SetOnce<ModelLoadingService> modelLoadingService) throws IOException {
        super(in, NAME);
        modelId = in.readString();
        bucketPathMap = in.readMap(StreamInput::readString, StreamInput::readString);
        inferenceConfig = in.readOptionalNamedWriteable(InferenceConfigUpdate.class);
        this.modelLoadingService = modelLoadingService;
        this.model = null;
        this.licenseState = licenseState;
        this.settings = settings;
    }

    /**
     * Constructor for the rewrite phase.
     */
    private InferencePipelineAggregationBuilder(
        String name,
        Map<String, String> bucketsPath,
        Supplier<LocalModel> model,
        String modelId,
        InferenceConfigUpdate inferenceConfig,
        XPackLicenseState licenseState,
        Settings settings
    ) {
        super(name, NAME, new TreeMap<>(bucketsPath).values().toArray(new String[] {}));
        modelLoadingService = null;
        bucketPathMap = bucketsPath;
        this.model = model;
        /*
         * These aren't strictly needed for running the pipeline aggregation
         * but are needed for serialization, which is still done after the
         * rewrite, mostly due to some oddness with the interaction with the
         * transport client. It *should* vanish once we no longer have to
         * support the transport client.
         */
        this.modelId = modelId;
        this.inferenceConfig = inferenceConfig;
        this.licenseState = licenseState;
        this.settings = settings;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public void setInferenceConfig(InferenceConfigUpdate inferenceConfig) {
        this.inferenceConfig = inferenceConfig;
    }

    @Override
    protected void validate(ValidationContext context) {
        context.validateHasParent(NAME, name);
        if (modelId == null) {
            context.addValidationError("[model_id] must be set");
        }

        if (inferenceConfig != null) {
            // error if the results field is set and not equal to the only acceptable value
            String resultsField = inferenceConfig.getResultsField();
            if (Strings.isNullOrEmpty(resultsField) == false && AGGREGATIONS_RESULTS_FIELD.equals(resultsField) == false) {
                context.addValidationError("setting option [" + ClassificationConfig.RESULTS_FIELD.getPreferredName()
                    + "] to [" + resultsField + "] is not valid for inference aggregations");
            }

            if (inferenceConfig instanceof ClassificationConfigUpdate) {
                ClassificationConfigUpdate classUpdate = (ClassificationConfigUpdate)inferenceConfig;

                // error if the top classes result field is set and not equal to the only acceptable value
                String topClassesField = classUpdate.getTopClassesResultsField();
                if (Strings.isNullOrEmpty(topClassesField) == false &&
                    ClassificationConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD.equals(topClassesField) == false) {
                    context.addValidationError("setting option [" + ClassificationConfig.DEFAULT_TOP_CLASSES_RESULTS_FIELD
                        + "] to [" + topClassesField + "] is not valid for inference aggregations");
                }
            }
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeMap(bucketPathMap, StreamOutput::writeString, StreamOutput::writeString);
        out.writeOptionalNamedWriteable(inferenceConfig);
    }

    @Override
    public InferencePipelineAggregationBuilder rewrite(QueryRewriteContext context) {
        if (model != null) {
            return this;
        }

        SetOnce<LocalModel> loadedModel = new SetOnce<>();
        BiConsumer<Client, ActionListener<?>> modelLoadAction = (client, listener) ->
            modelLoadingService.get().getModelForSearch(modelId, listener.delegateFailure((delegate, model) -> {
                loadedModel.set(model);

                boolean isLicensed = licenseState.checkFeature(XPackLicenseState.Feature.MACHINE_LEARNING) ||
                    licenseState.isAllowedByLicense(model.getLicenseLevel());
                if (isLicensed) {
                    delegate.onResponse(null);
                } else {
                    delegate.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
                }
            }));


        context.registerAsyncAction((client, listener) -> {
            if (XPackSettings.SECURITY_ENABLED.get(settings)) {
                // check the user has ml privileges
                SecurityContext securityContext = new SecurityContext(settings, client.threadPool().getThreadContext());
                useSecondaryAuthIfAvailable(securityContext, () -> {
                    final String username = securityContext.getUser().principal();
                    final HasPrivilegesRequest privRequest = new HasPrivilegesRequest();
                    privRequest.username(username);
                    privRequest.clusterPrivileges(GetTrainedModelsAction.NAME);
                    privRequest.indexPrivileges(new RoleDescriptor.IndicesPrivileges[]{});
                    privRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[]{});

                    ActionListener<HasPrivilegesResponse> privResponseListener = ActionListener.wrap(
                        r -> {
                            if (r.isCompleteMatch()) {
                                modelLoadAction.accept(client, listener);
                            } else {
                                listener.onFailure(Exceptions.authorizationError("user [" + username
                                    + "] does not have the privilege to get trained models so cannot use ml inference"));
                            }
                        },
                        listener::onFailure);

                    client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
                });
            } else {
                modelLoadAction.accept(client, listener);
            }
        });
        return new InferencePipelineAggregationBuilder(name, bucketPathMap, loadedModel::get, modelId, inferenceConfig, licenseState,
            settings);
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) {
        if (model == null) {
            throw new IllegalStateException("model must be null, missing rewrite?");
        }
        InferenceConfigUpdate update = adaptForAggregation(inferenceConfig);
        return new InferencePipelineAggregator(name, bucketPathMap, metaData, update, model.get());
    }

    static InferenceConfigUpdate adaptForAggregation(InferenceConfigUpdate originalUpdate) {
        InferenceConfigUpdate updated;
        if (originalUpdate == null) {
            updated = new ResultsFieldUpdate(AGGREGATIONS_RESULTS_FIELD);
        } else {
            // Create an update that changes the default results field.
            // This isn't necessary for top classes as the default is the same one used here
            updated = originalUpdate.newBuilder().setResultsField(AGGREGATIONS_RESULTS_FIELD).build();
        }

        return updated;
    }

    @Override
    protected boolean overrideBucketsPath() {
        return true;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID.getPreferredName(), modelId);
        builder.field(BUCKETS_PATH_FIELD.getPreferredName(), bucketPathMap);
        if (inferenceConfig != null) {
            builder.startObject(INFERENCE_CONFIG.getPreferredName());
            builder.field(inferenceConfig.getName(), inferenceConfig);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), bucketPathMap, modelId, inferenceConfig);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InferencePipelineAggregationBuilder other = (InferencePipelineAggregationBuilder) obj;
        return Objects.equals(bucketPathMap, other.bucketPathMap)
            && Objects.equals(modelId, other.modelId)
            && Objects.equals(inferenceConfig, other.inferenceConfig);
    }
}
