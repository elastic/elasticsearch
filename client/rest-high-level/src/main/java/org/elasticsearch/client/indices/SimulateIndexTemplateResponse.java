/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.indices;

import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SimulateIndexTemplateResponse {

    private static final ParseField TEMPLATE = new ParseField("template");
    private static final ParseField OVERLAPPING = new ParseField("overlapping");
    private static final ParseField NAME = new ParseField("name");
    private static final ParseField INDEX_PATTERNS = new ParseField("index_patterns");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SimulateIndexTemplateResponse, Void> PARSER =
        new ConstructingObjectParser<>("simulate_index_templates_response", false,
            a -> new SimulateIndexTemplateResponse(
                a[0] != null ? (Template) a[0] : null,
                a[1] != null ?
                    ((List<IndexTemplateAndPatterns>) a[1]).stream()
                        .collect(Collectors.toMap(IndexTemplateAndPatterns::name, IndexTemplateAndPatterns::indexPatterns)) : null
            )
        );

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<IndexTemplateAndPatterns, Void> INNER_PARSER =
        new ConstructingObjectParser<>("index_template_and_patterns", false,
            a -> new IndexTemplateAndPatterns((String) a[0], (List<String>) a[1]));

    private static class IndexTemplateAndPatterns {
        String name;
        List<String> indexPatterns;

        IndexTemplateAndPatterns(String name, List<String> indexPatterns) {
            this.name = name;
            this.indexPatterns = indexPatterns;
        }

        public String name() {
            return name;
        }

        public List<String> indexPatterns() {
            return indexPatterns;
        }
    }

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), Template.PARSER, TEMPLATE);
        INNER_PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        INNER_PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), INDEX_PATTERNS);
        PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), INNER_PARSER, OVERLAPPING);
    }

    @Nullable
    // the resolved settings, mappings and aliases for the matched templates, if any
    private Template resolvedTemplate;

    @Nullable
    // a map of template names and their index patterns that would overlap when matching the given index name
    private Map<String, List<String>> overlappingTemplates;

    SimulateIndexTemplateResponse(@Nullable Template resolvedTemplate, @Nullable Map<String, List<String>> overlappingTemplates) {
        this.resolvedTemplate = resolvedTemplate;
        this.overlappingTemplates = overlappingTemplates;
    }

    public static SimulateIndexTemplateResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public Template resolvedTemplate() {
        return resolvedTemplate;
    }

    public Map<String, List<String>> overlappingTemplates() {
        return overlappingTemplates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimulateIndexTemplateResponse that = (SimulateIndexTemplateResponse) o;
        return Objects.equals(resolvedTemplate, that.resolvedTemplate)
            && Objects.deepEquals(overlappingTemplates, that.overlappingTemplates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(resolvedTemplate, overlappingTemplates);
    }

    @Override
    public String toString() {
        return "SimulateIndexTemplateResponse{" + "resolved template=" + resolvedTemplate + ", overlapping templates="
            + String.join("|", overlappingTemplates.keySet()) + "}";
    }
}
