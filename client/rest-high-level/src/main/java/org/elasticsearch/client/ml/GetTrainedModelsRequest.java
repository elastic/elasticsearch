/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.ml.inference.TrainedModelConfig;
import org.elasticsearch.core.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class GetTrainedModelsRequest implements Validatable {

    private static final String DEFINITION = "definition";
    private static final String TOTAL_FEATURE_IMPORTANCE = "total_feature_importance";
    private static final String FEATURE_IMPORTANCE_BASELINE = "feature_importance_baseline";
    public static final String ALLOW_NO_MATCH = "allow_no_match";
    public static final String EXCLUDE_GENERATED = "exclude_generated";
    public static final String DECOMPRESS_DEFINITION = "decompress_definition";
    public static final String TAGS = "tags";
    public static final String INCLUDE = "include";

    private final List<String> ids;
    private Boolean allowNoMatch;
    private Set<String> includes = new HashSet<>();
    private Boolean decompressDefinition;
    private Boolean excludeGenerated;
    private PageParams pageParams;
    private List<String> tags;

    /**
     * Helper method to create a request that will get ALL TrainedModelConfigs
     * @return new {@link GetTrainedModelsRequest} object for the id "_all"
     */
    public static GetTrainedModelsRequest getAllTrainedModelConfigsRequest() {
        return new GetTrainedModelsRequest("_all");
    }

    public GetTrainedModelsRequest(String... ids) {
        this.ids = Arrays.asList(ids);
    }

    public List<String> getIds() {
        return ids;
    }

    public Boolean getAllowNoMatch() {
        return allowNoMatch;
    }

    /**
     * Whether to ignore if a wildcard expression matches no trained models.
     *
     * @param allowNoMatch If this is {@code false}, then an error is returned when a wildcard (or {@code _all})
     *                    does not match any trained models
     */
    public GetTrainedModelsRequest setAllowNoMatch(boolean allowNoMatch) {
        this.allowNoMatch = allowNoMatch;
        return this;
    }

    public PageParams getPageParams() {
        return pageParams;
    }

    public GetTrainedModelsRequest setPageParams(@Nullable PageParams pageParams) {
        this.pageParams = pageParams;
        return this;
    }

    public Set<String> getIncludes() {
        return Collections.unmodifiableSet(includes);
    }

    public GetTrainedModelsRequest includeDefinition() {
        this.includes.add(DEFINITION);
        return this;
    }

    public GetTrainedModelsRequest includeTotalFeatureImportance() {
        this.includes.add(TOTAL_FEATURE_IMPORTANCE);
        return this;
    }

    public GetTrainedModelsRequest includeFeatureImportanceBaseline() {
        this.includes.add(FEATURE_IMPORTANCE_BASELINE);
        return this;
    }

    /**
     * Whether to include the full model definition.
     *
     * The full model definition can be very large.
     * @deprecated Use {@link GetTrainedModelsRequest#includeDefinition()}
     * @param includeDefinition If {@code true}, the definition is included.
     */
    @Deprecated
    public GetTrainedModelsRequest setIncludeDefinition(Boolean includeDefinition) {
        if (includeDefinition != null && includeDefinition) {
            return this.includeDefinition();
        }
        return this;
    }

    public Boolean getDecompressDefinition() {
        return decompressDefinition;
    }

    /**
     * Whether or not to decompress the trained model, or keep it in its compressed string form
     *
     * @param decompressDefinition If {@code true}, the definition is decompressed.
     */
    public GetTrainedModelsRequest setDecompressDefinition(Boolean decompressDefinition) {
        this.decompressDefinition = decompressDefinition;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    /**
     * The tags that the trained model must match. These correspond to {@link TrainedModelConfig#getTags()}.
     *
     * The models returned will match ALL tags supplied.
     * If none are provided, only the provided ids are used to find models
     * @param tags The tags to match when finding models
     */
    public GetTrainedModelsRequest setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    /**
     * See {@link GetTrainedModelsRequest#setTags(List)}
     */
    public GetTrainedModelsRequest setTags(String... tags) {
        return setTags(Arrays.asList(tags));
    }

    public Boolean getExcludeGenerated() {
        return excludeGenerated;
    }

    /**
     * Setting this flag to `true` removes certain fields from the model definition on retrieval.
     *
     * This is useful when getting the model and wanting to put it in another cluster.
     *
     * Default value is false.
     * @param excludeGenerated Boolean value indicating if certain fields should be removed from the mode on GET
     */
    public GetTrainedModelsRequest setExcludeGenerated(Boolean excludeGenerated) {
        this.excludeGenerated = excludeGenerated;
        return this;
    }

    @Override
    public Optional<ValidationException> validate() {
        if (ids == null || ids.isEmpty()) {
            return Optional.of(ValidationException.withError("trained model id must not be null"));
        }
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GetTrainedModelsRequest other = (GetTrainedModelsRequest) o;
        return Objects.equals(ids, other.ids)
            && Objects.equals(allowNoMatch, other.allowNoMatch)
            && Objects.equals(decompressDefinition, other.decompressDefinition)
            && Objects.equals(includes, other.includes)
            && Objects.equals(excludeGenerated, other.excludeGenerated)
            && Objects.equals(pageParams, other.pageParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ids, allowNoMatch, pageParams, decompressDefinition, includes, excludeGenerated);
    }
}
