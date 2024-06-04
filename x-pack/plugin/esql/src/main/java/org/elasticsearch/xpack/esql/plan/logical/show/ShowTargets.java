/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.show;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.index.IndexResolver;
import org.elasticsearch.xpack.esql.core.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.index.IndexResolver.IndexType.VALID_REGULAR;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.splitQualifiedIndex;

public class ShowTargets extends LeafPlan {

    public static final String PATTERN_MATCH_ALL = "*";

    private static final List<Attribute> ATTRIBUTES = Stream.of("cluster", "name", "type")
        .map(name -> (Attribute) new ReferenceAttribute(Source.EMPTY, name, DataType.KEYWORD))
        .toList();

    private final String pattern;

    public ShowTargets(Source source, @Nullable String pattern) {
        super(source);
        this.pattern = pattern != null ? pattern : PATTERN_MATCH_ALL;
    }

    @Override
    public List<Attribute> output() {
        return ATTRIBUTES;
    }

    public LocalSupplier supplier(IndexResolver indexResolver) {
        return new ShowSupplier(indexResolver) {
            @Override
            void resolvePattern(IndexResolver indexResolver) {
                Tuple<String, String> split = splitQualifiedIndex(pattern);
                indexResolver.resolveNames(split.v1(), split.v2(), null, VALID_REGULAR, new SupplierActionListener<>() {
                    @Override
                    public void onResult(Set<IndexResolver.IndexInfo> indexInfos) {
                        indexInfos.forEach(indexInfo -> {
                            List<Object> row = new ArrayList<>(ATTRIBUTES.size());
                            row.add(new BytesRef(indexInfo.cluster()));
                            row.add(new BytesRef(indexInfo.name()));
                            row.add(new BytesRef(indexInfo.type().toNative()));
                            addRow(row);
                        });
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
        return NodeInfo.create(this, ShowTargets::new, pattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return obj instanceof ShowTargets other && Objects.equals(pattern, other.pattern);
    }
}
