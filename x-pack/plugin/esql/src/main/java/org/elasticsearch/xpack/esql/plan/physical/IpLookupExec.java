/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class IpLookupExec extends UnaryExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "IpLookupExec",
        IpLookupExec::new
    );

    private final Expression ipAddress;
    private final String databaseFile;
    private final List<Attribute> resolvedGeoLocationFields;
    private final Map<String, DataType> geoLocationFieldTemplates;
    private List<Attribute> lazyOutput;

    public IpLookupExec(
        Source source,
        PhysicalPlan child,
        Expression ipAddress,
        String databaseFile,
        List<Attribute> resolvedGeoLocationFields,
        Map<String, DataType> geoLocationFieldTemplates
    ) {
        super(source, child);
        this.ipAddress = ipAddress;
        this.databaseFile = databaseFile;
        this.resolvedGeoLocationFields = resolvedGeoLocationFields;
        this.geoLocationFieldTemplates = geoLocationFieldTemplates;
    }

    public IpLookupExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalString(),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readOrderedMap(StreamInput::readString, i -> i.readEnum(DataType.class))
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(ipAddress);
        out.writeOptionalString(databaseFile);
        out.writeNamedWriteableCollection(resolvedGeoLocationFields);
        out.writeMap(geoLocationFieldTemplates, StreamOutput::writeString, StreamOutput::writeEnum);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression ipAddress() {
        return ipAddress;
    }

    public String databaseFile() {
        return databaseFile;
    }

    public List<Attribute> resolvedGeoLocationFields() {
        return resolvedGeoLocationFields;
    }

    public Map<String, DataType> geoLocationFieldTemplates() {
        return geoLocationFieldTemplates;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(resolvedGeoLocationFields, child().output());
        }
        return lazyOutput;
    }

    @Override
    protected AttributeSet computeReferences() {
        return ipAddress.references();
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new IpLookupExec(source(), newChild, ipAddress, databaseFile, resolvedGeoLocationFields, geoLocationFieldTemplates);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(
            this,
            IpLookupExec::new,
            child(),
            ipAddress,
            databaseFile,
            resolvedGeoLocationFields,
            geoLocationFieldTemplates
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), ipAddress, databaseFile, resolvedGeoLocationFields, geoLocationFieldTemplates);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (super.equals(obj) == false) {
            return false;
        }
        IpLookupExec other = (IpLookupExec) obj;
        return Objects.equals(ipAddress, other.ipAddress)
            && Objects.equals(databaseFile, other.databaseFile)
            && Objects.equals(resolvedGeoLocationFields, other.resolvedGeoLocationFields)
            && Objects.equals(geoLocationFieldTemplates, other.geoLocationFieldTemplates);
    }
}
