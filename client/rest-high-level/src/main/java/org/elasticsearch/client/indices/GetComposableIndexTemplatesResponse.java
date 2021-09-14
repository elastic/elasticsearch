/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.indices;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


public class GetComposableIndexTemplatesResponse {

    public static final ParseField NAME = new ParseField("name");
    public static final ParseField INDEX_TEMPLATES = new ParseField("index_templates");
    public static final ParseField INDEX_TEMPLATE = new ParseField("index_template");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Map<String, ComposableIndexTemplate>, Void> PARSER =
        new ConstructingObjectParser<>("index_templates", false,
            a -> ((List<NamedIndexTemplate>) a[0]).stream().collect(Collectors.toMap(n -> n.name, n -> n.indexTemplate,
                (n1, n2) -> n1, LinkedHashMap::new)));

    private static final ConstructingObjectParser<NamedIndexTemplate, Void> INNER_PARSER =
        new ConstructingObjectParser<>("named_index_template", false,
            a -> new NamedIndexTemplate((String) a[0], (ComposableIndexTemplate) a[1]));

    static {
        INNER_PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        INNER_PARSER.declareObject(ConstructingObjectParser.constructorArg(), ComposableIndexTemplate.PARSER, INDEX_TEMPLATE);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), INNER_PARSER, INDEX_TEMPLATES);
    }

    private static class NamedIndexTemplate {
        String name;
        ComposableIndexTemplate indexTemplate;

        private NamedIndexTemplate(String name, ComposableIndexTemplate indexTemplate) {
            this.name = name;
            this.indexTemplate = indexTemplate;
        }
    }

    @Override
    public String toString() {
        return "GetIndexTemplatesResponse [indexTemplates=" + indexTemplates + "]";
    }

    private final Map<String, ComposableIndexTemplate> indexTemplates;

    GetComposableIndexTemplatesResponse(Map<String, ComposableIndexTemplate> indexTemplates) {
        this.indexTemplates = Collections.unmodifiableMap(new LinkedHashMap<>(indexTemplates));
    }

    public Map<String, ComposableIndexTemplate> getIndexTemplates() {
        return indexTemplates;
    }


    public static GetComposableIndexTemplatesResponse fromXContent(XContentParser parser) throws IOException {
        return new GetComposableIndexTemplatesResponse(PARSER.apply(parser, null));
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexTemplates);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetComposableIndexTemplatesResponse other = (GetComposableIndexTemplatesResponse) obj;
        return Objects.equals(indexTemplates, other.indexTemplates);
    }


}
