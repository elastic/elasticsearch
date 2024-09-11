/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Map;

/**
 * This extends BulkRequest with support for providing substitute pipeline definitions and template definitions. In a user request, the
 * substitutions will look something like this:
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
 *   "template_substitutions": {
 *     "my-template-1": {
 *       "settings": {
 *         "number_of_shards": 1
 *       },
 *       "mappings": {
 *         "_source": {
 *           "enabled": false
 *         },
 *         "properties": {
 *           "host_name": {
 *             "type": "keyword"
 *           },
 *           "created_at": {
 *             "type": "date",
 *             "format": "EEE MMM dd HH:mm:ss Z yyyy"
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
    private final Map<String, Map<String, Object>> templateSubstitutions;

    /**
     * @param pipelineSubstitutions The pipeline definitions that are to be used in place of any pre-existing pipeline definitions with
     *                              the same pipelineId. The key of the map is the pipelineId, and the value the pipeline definition as
     *                              parsed by XContentHelper.convertToMap().
     */
    public SimulateBulkRequest(
        @Nullable Map<String, Map<String, Object>> pipelineSubstitutions,
        @Nullable Map<String, Map<String, Object>> templateSubstitutions
    ) {
        super();
        this.pipelineSubstitutions = pipelineSubstitutions;
        this.templateSubstitutions = templateSubstitutions;
    }

    @SuppressWarnings("unchecked")
    public SimulateBulkRequest(StreamInput in) throws IOException {
        super(in);
        this.pipelineSubstitutions = (Map<String, Map<String, Object>>) in.readGenericValue();
        if (in.getTransportVersion().onOrAfter(TransportVersions.SIMULATE_TEMPLATES_SUBSTITUTIONS)) {
            this.templateSubstitutions = (Map<String, Map<String, Object>>) in.readGenericValue();
        } else {
            templateSubstitutions = Map.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericValue(pipelineSubstitutions);
        if (out.getTransportVersion().onOrAfter(TransportVersions.SIMULATE_TEMPLATES_SUBSTITUTIONS)) {
            out.writeGenericValue(templateSubstitutions);
        }
    }

    public Map<String, Map<String, Object>> getPipelineSubstitutions() {
        return pipelineSubstitutions;
    }

    public Map<String, Map<String, Object>> getTemplateSubstitutions() {
        return templateSubstitutions;
    }

    @Override
    public boolean isSimulated() {
        return true;
    }
}
