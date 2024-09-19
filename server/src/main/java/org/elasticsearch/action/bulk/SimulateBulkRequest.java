/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This extends BulkRequest with support for providing substitute pipeline definitions and component template definitions. In a user
 * request, the substitutions will look something like this:
 *
 *   "pipeline_substitutions": {
 *     "my-pipeline-1": {
 *       "processors": [
 *         {
 *           "set": {
 *             "field": "my-new-boolean-field",
 *             "value": true
 *           }
 *         }
 *       ]
 *     },
 *     "my-pipeline-2": {
 *       "processors": [
 *         {
 *           "set": {
 *             "field": "my-new-boolean-field",
 *             "value": true
 *           },
 *           "rename": {
 *               "field": "old_field",
 *               "target_field": "new field"
 *           }
 *         }
 *       ]
 *     }
 *   },
 *   "component_template_substitutions": {
 *     "my-template-1": {
 *       "template": {
 *         "settings": {
 *           "number_of_shards": 1
 *         },
 *         "mappings": {
 *           "_source": {
 *             "enabled": false
 *           },
 *           "properties": {
 *             "host_name": {
 *               "type": "keyword"
 *             },
 *             "created_at": {
 *               "type": "date",
 *               "format": "EEE MMM dd HH:mm:ss Z yyyy"
 *             }
 *           }
 *         }
 *       }
 *     }
 *   }
 *
 *   The pipelineSubstitutions Map held by this class is intended to be the result of XContentHelper.convertToMap(). The top-level keys
 *   are the pipelineIds ("my-pipeline-1" and "my-pipeline-2" in the example above). The values are the Maps of "processors" to the List of
 *   processor definitions.
 */
public class SimulateBulkRequest extends BulkRequest {
    private final Map<String, Map<String, Object>> pipelineSubstitutions;
    private final Map<String, Map<String, Object>> componentTemplateSubstitutions;

    /**
     * @param pipelineSubstitutions The pipeline definitions that are to be used in place of any pre-existing pipeline definitions with
     *                              the same pipelineId. The key of the map is the pipelineId, and the value the pipeline definition as
     *                              parsed by XContentHelper.convertToMap().
     * @param componentTemplateSubstitutions The component template definitions that are to be used in place of any pre-existing
     *                                       component template definitions with the same name.
     */
    public SimulateBulkRequest(
        @Nullable Map<String, Map<String, Object>> pipelineSubstitutions,
        @Nullable Map<String, Map<String, Object>> componentTemplateSubstitutions
    ) {
        super();
        this.pipelineSubstitutions = pipelineSubstitutions;
        this.componentTemplateSubstitutions = componentTemplateSubstitutions;
    }

    @SuppressWarnings("unchecked")
    public SimulateBulkRequest(StreamInput in) throws IOException {
        super(in);
        this.pipelineSubstitutions = (Map<String, Map<String, Object>>) in.readGenericValue();
        if (in.getTransportVersion().onOrAfter(TransportVersions.SIMULATE_COMPONENT_TEMPLATES_SUBSTITUTIONS)) {
            this.componentTemplateSubstitutions = (Map<String, Map<String, Object>>) in.readGenericValue();
        } else {
            componentTemplateSubstitutions = Map.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericValue(pipelineSubstitutions);
        if (out.getTransportVersion().onOrAfter(TransportVersions.SIMULATE_COMPONENT_TEMPLATES_SUBSTITUTIONS)) {
            out.writeGenericValue(componentTemplateSubstitutions);
        }
    }

    public Map<String, Map<String, Object>> getPipelineSubstitutions() {
        return pipelineSubstitutions;
    }

    @Override
    public boolean isSimulated() {
        return true;
    }

    @Override
    public Map<String, ComponentTemplate> getComponentTemplateSubstitutions() throws IOException {
        if (componentTemplateSubstitutions == null) {
            return Map.of();
        }
        Map<String, ComponentTemplate> result = new HashMap<>(componentTemplateSubstitutions.size());
        for (Map.Entry<String, Map<String, Object>> rawEntry : componentTemplateSubstitutions.entrySet()) {
            result.put(rawEntry.getKey(), convertRawTemplateToComponentTemplate(rawEntry.getValue()));
        }
        return result;
    }

    private static ComponentTemplate convertRawTemplateToComponentTemplate(Map<String, Object> rawTemplate) throws IOException {
        ComponentTemplate componentTemplate;
        try (var parser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, rawTemplate)) {
            componentTemplate = ComponentTemplate.parse(parser);
        }
        return componentTemplate;
    }

    @Override
    public BulkRequest shallowClone() {
        BulkRequest bulkRequest = new SimulateBulkRequest(pipelineSubstitutions, componentTemplateSubstitutions);
        bulkRequest.setRefreshPolicy(getRefreshPolicy());
        bulkRequest.waitForActiveShards(waitForActiveShards());
        bulkRequest.timeout(timeout());
        bulkRequest.pipeline(pipeline());
        bulkRequest.routing(routing());
        bulkRequest.requireAlias(requireAlias());
        bulkRequest.requireDataStream(requireDataStream());
        return bulkRequest;
    }
}
