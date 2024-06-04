/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.show;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.index.IndexResolution;
import org.elasticsearch.xpack.esql.core.index.IndexResolver;
import org.elasticsearch.xpack.esql.core.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.plan.logical.show.ShowTargets.PATTERN_MATCH_ALL;

public class ShowFields extends LeafPlan {

    private static final List<Attribute> ATTRIBUTES = Stream.of("name", "type")
        .map(name -> (Attribute) new ReferenceAttribute(Source.EMPTY, name, DataType.KEYWORD))
        .toList();

    private final String target;

    public ShowFields(Source source, @Nullable String target) {
        super(source);
        this.target = target != null ? target : PATTERN_MATCH_ALL;
    }

    @Override
    public List<Attribute> output() {
        return ATTRIBUTES;
    }

    public LocalSupplier supplier(IndexResolver indexResolver) {
        return new ShowSupplier(indexResolver) {
            @Override
            void resolvePattern(IndexResolver indexResolver) {
                indexResolver.resolveAsMergedMapping(target, IndexResolver.ALL_FIELDS, false, Map.of(), new SupplierActionListener<>() {
                    @Override
                    public void onResult(IndexResolution indexResolution) {
                        if (indexResolution.isValid()) {
                            walkMapping(indexResolution.get().mapping(), null);
                        }
                    }

                    private void walkMapping(Map<String, EsField> mapping, String prefix) {
                        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
                            EsField field = entry.getValue();
                            DataType fieldType = field.getDataType();
                            if (fieldType != null) { // non-object, metadata
                                List<Object> row = new ArrayList<>(ATTRIBUTES.size());
                                String fieldName = entry.getKey();
                                String qualified = prefix != null ? prefix + "." + fieldName : fieldName;
                                row.add(new BytesRef(qualified));
                                row.add(new BytesRef(fieldType.typeName()));
                                addRow(row);

                                walkMapping(field.getProperties(), fieldName);
                            }
                        }
                    }
                });
            }
        };
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, ShowFields::new, target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(target);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof ShowFields other && Objects.equals(target, other.target);
    }

}
