/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.EsField;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LocalRelation extends LeafPlan {

    private final List<Attribute> output;
    private final LocalSupplier supplier;

    public LocalRelation(Source source, List<Attribute> output, LocalSupplier supplier) {
        super(source);
        this.output = output;
        this.supplier = supplier;
    }

    @Override
    protected NodeInfo<LocalRelation> info() {
        return NodeInfo.create(this, LocalRelation::new, output, supplier);
    }

    public LocalSupplier supplier() {
        return supplier;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, supplier);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LocalRelation other = (LocalRelation) obj;
        return Objects.equals(supplier, other.supplier) && Objects.equals(output, other.output);
    }

    public static LocalRelation.FromConfig fromConfig(Source source, EsqlConfiguration config, String name) {
        Map<String, Column> table = config.tables().get(name);
        Block[] blocks = new Block[table.size()];

        List<Attribute> attributes = new ArrayList<>(blocks.length);
        int i = 0;
        for (Map.Entry<String, Column> entry : table.entrySet()) {
            String columnName = entry.getKey();
            Column column = entry.getValue();
            // create a fake ES field - alternative is to use a ReferenceAttribute
            EsField field = new EsField(columnName, column.type(), null, false, false);
            attributes.add(new FieldAttribute(source, null, name, field));
            // prepare the block for the supplier
            blocks[i++] = column.values();
        }
        LocalSupplier supplier = LocalSupplier.of(blocks);
        return new LocalRelation.FromConfig(source, attributes, supplier, name);
    }

    public static class FromConfig extends LocalRelation {
        private final String name;

        private FromConfig(Source source, List<Attribute> output, LocalSupplier supplier, String name) {
            super(source, output, supplier);
            this.name = name;
        }

        public static FromConfig readFrom(PlanStreamInput in) throws IOException {
            Source source = in.readSource();
            String name = in.readString();
            return fromConfig(source, in.configuration(), name);
        }

        public void writeTo(PlanStreamOutput out) throws IOException {
            out.writeSource(source());
            out.writeString(name);
        }

        @Override
        public boolean equals(Object obj) {
            if (super.equals(obj) == false) {
                return false;
            }
            LocalRelation.FromConfig other = (FromConfig) obj;
            return name.equals(other.name);
        }
    }
}
