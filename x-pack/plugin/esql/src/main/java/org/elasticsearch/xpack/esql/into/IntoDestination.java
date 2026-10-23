/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Where an {@code INTO} query writes, resolved from the cluster metadata on the coordinator before anything is read,
 * so that a query that cannot be written is refused before it writes part of itself.
 * <p>
 * The destination has to exist, or an index template has to describe what it would look like: a name matching
 * neither would otherwise be created by the write, dynamically mapped from whatever the first rows held, so a
 * mistyped name would succeed. Columns the destination does not map are allowed only where its mapping allows
 * dynamic fields; {@code dynamic: false} would silently drop them and {@code dynamic: strict} would refuse every row.
 */
public final class IntoDestination {

    private static final String DYNAMIC = "dynamic";
    private static final String PROPERTIES = "properties";

    private final String name;
    private final boolean dataStream;
    /** The destination's mapping as a map, or null when it has none. */
    @Nullable
    private final Map<String, Object> mapping;

    IntoDestination(String name, boolean dataStream, @Nullable Map<String, Object> mapping) {
        this.name = name;
        this.dataStream = dataStream;
        this.mapping = mapping;
    }

    public String name() {
        return name;
    }

    /**
     * Whether writes go to a data stream. Data streams are append-only to the bulk API: by name they take only
     * {@code create} ops, and their backing indices refuse an {@code index} op unless it carries the sequence number
     * and primary term of the document it replaces, which would take a lookup per row. Rather than replace in only
     * the write index that way, {@code INTO} does not let an id column through to a data stream at all, and refuses
     * {@code collisions=replace} for one: the stream generates the ids, so nothing a query names can be replaced.
     * A time series stream derives its ids on the shard, so rows there can still collide; {@code keep} counts them.
     */
    public boolean isDataStream() {
        return dataStream;
    }

    /**
     * Resolves a destination name to the index that will be written, or to the template that will shape it.
     *
     * @throws VerificationException when neither exists, or an alias has no write index
     */
    public static IntoDestination resolve(ProjectMetadata project, String name) {
        IndexAbstraction abstraction = project.getIndicesLookup().get(name);
        if (abstraction != null) {
            Index writeIndex = abstraction.getWriteIndex();
            if (writeIndex == null) {
                throw new VerificationException("[{}] is an alias without a write index, so INTO cannot write to it", name);
            }
            IndexMetadata index = project.index(writeIndex);
            MappingMetadata mapping = index == null ? null : index.mapping();
            // Writing straight to a backing index is a plain index write, so only the stream itself (or an alias of one) counts.
            boolean dataStream = abstraction.getType() != IndexAbstraction.Type.CONCRETE_INDEX && abstraction.isDataStreamRelated();
            return new IntoDestination(name, dataStream, mapping == null ? null : mapping.sourceAsMap());
        }
        String templateName = MetadataIndexTemplateService.findV2Template(project, name, false);
        if (templateName == null) {
            throw new VerificationException(
                "[{}] does not exist and no index template matches it; INTO writes to a predefined destination, "
                    + "not one derived from the rows written to it",
                name
            );
        }
        ComposableIndexTemplate template = project.templatesV2().get(templateName);
        return new IntoDestination(name, template.getDataStreamTemplate() != null, templateMapping(project, templateName, name));
    }

    /**
     * The mapping a template would give the destination. The template's fragments are merged by layering each over the
     * previous one, which is how index creation resolves them but without the full mapper machinery: good enough to
     * know which fields exist and whether dynamic fields are allowed.
     */
    @Nullable
    private static Map<String, Object> templateMapping(ProjectMetadata project, String templateName, String indexName) {
        Map<String, Object> merged = null;
        for (CompressedXContent fragment : MetadataIndexTemplateService.collectMappings(project, templateName, indexName)) {
            Map<String, Object> parsed = XContentHelper.convertToMap(fragment.uncompressed(), false, XContentType.JSON).v2();
            if (parsed.size() == 1 && parsed.containsKey(MapperServiceNames.SINGLE_MAPPING_NAME)) {
                parsed = castMap(parsed.get(MapperServiceNames.SINGLE_MAPPING_NAME));
            }
            merged = merged == null ? parsed : merge(merged, parsed);
        }
        return merged;
    }

    /** Layers {@code over} on top of {@code base}: nested maps are merged, anything else is replaced. */
    private static Map<String, Object> merge(Map<String, Object> base, Map<String, Object> over) {
        Map<String, Object> result = new HashMap<>(base);
        for (Map.Entry<String, Object> entry : over.entrySet()) {
            Object existing = result.get(entry.getKey());
            if (existing instanceof Map<?, ?> && entry.getValue() instanceof Map<?, ?>) {
                result.put(entry.getKey(), merge(castMap(existing), castMap(entry.getValue())));
            } else {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    /**
     * Refuses an output the destination cannot take: metadata columns, unsupported types, a column the mapping does not
     * know where dynamic fields are not allowed, and an id column or {@code collisions=replace} going to a data stream
     * (see {@link #isDataStream()}).
     *
     * @throws VerificationException describing the first problem found
     */
    public void validate(List<Attribute> output, IntoOptions options) {
        if (dataStream && options.collisions() == IntoOptions.Collisions.REPLACE) {
            throw new VerificationException(
                "[{}] is a data stream, which only takes new documents, so INTO cannot replace in it; "
                    + "use \"collisions\": \"keep\" or \"collisions\": \"abort\"",
                name
            );
        }
        for (Attribute attribute : output) {
            String column = attribute.name();
            if (IntoPageWriter.ID_COLUMN.equals(column)) {
                if (dataStream) {
                    throw new VerificationException(
                        "[{}] cannot be written to [{}]: a data stream generates its own document ids; drop the column with DROP or KEEP",
                        column,
                        name
                    );
                }
                if (DataType.isString(attribute.dataType()) == false) {
                    throw new VerificationException(
                        "[{}] is used as the document id, so it must be a keyword or text column, not [{}]",
                        column,
                        attribute.dataType().typeName()
                    );
                }
                continue;
            }
            if (attribute instanceof MetadataAttribute || MetadataAttribute.isSupported(column)) {
                throw new VerificationException("metadata column [{}] cannot be written by INTO; drop it with DROP or KEEP", column);
            }
            if (attribute.dataType() == DataType.UNSUPPORTED) {
                throw new VerificationException("[{}] has an unsupported type and cannot be written by INTO", column);
            }
            checkMapped(column);
        }
    }

    /**
     * Walks the mapping down the column's dotted path. A column that is mapped is fine; one that is not is fine only
     * where the nearest enclosing object (or the root) allows dynamic fields.
     */
    private void checkMapped(String column) {
        if (mapping == null) {
            // Nothing mapped yet, so the first write maps everything dynamically.
            return;
        }
        Object dynamic = mapping.get(DYNAMIC);
        Map<String, Object> level = mapping;
        String[] path = column.split("\\.");
        for (int i = 0; i < path.length; i++) {
            Map<String, Object> properties = level.get(PROPERTIES) instanceof Map<?, ?> ? castMap(level.get(PROPERTIES)) : null;
            Object field = properties == null ? null : properties.get(path[i]);
            if (field instanceof Map<?, ?> == false) {
                if (allowsDynamicFields(dynamic) == false) {
                    throw new VerificationException(
                        "[{}] is not mapped in [{}] and its mapping does not allow dynamic fields ({}={})",
                        column,
                        name,
                        DYNAMIC,
                        dynamic
                    );
                }
                return;
            }
            level = castMap(field);
            if (level.containsKey(DYNAMIC)) {
                dynamic = level.get(DYNAMIC);
            }
        }
    }

    /** Whether a {@code dynamic} mapping value lets new fields in: absent, {@code true} and {@code runtime} do. */
    static boolean allowsDynamicFields(@Nullable Object dynamic) {
        if (dynamic == null) {
            return true;
        }
        if (dynamic instanceof Boolean allowed) {
            return allowed;
        }
        return switch (dynamic.toString().toLowerCase(Locale.ROOT)) {
            case "true", "runtime" -> true;
            default -> false;
        };
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object value) {
        // Mappings parsed from JSON only ever hold string keys.
        return (Map<String, Object>) value;
    }

    /** The one mapping type name, kept here so this class does not depend on the mapper service. */
    private static final class MapperServiceNames {
        static final String SINGLE_MAPPING_NAME = "_doc";
    }
}
