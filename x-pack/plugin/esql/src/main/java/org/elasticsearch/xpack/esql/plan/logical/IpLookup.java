/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * A {@code IpLookup} is a logical plan node that represents the ESQL {@code IP_LOOKUP} command. It takes an IP address expression,
 * looks up geolocation information, and adds the results to the output as new attributes.
 * The attributes added are based on the specified database file, and they are all prefixed with the target field name.
 * If the IP address is null or invalid, all generated fields will be null.
 * It is the caller's responsibility to ensure that the database file exists and is accessible. If this plan is constructed, it is assumed
 * that the database file is valid and available.
 */
public class IpLookup extends UnaryPlan implements PostAnalysisVerificationAware, TelemetryAware, GeneratingPlan<IpLookup> {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "IpLookup", IpLookup::new);

    private final Expression ipAddress;
    private final Attribute targetField;
    private final String databaseFile;
    private final List<Attribute> resolvedGeoLocationFields;
    private final Map<String, DataType> geoLocationFieldTemplates;

    /**
     * There is seemingly a lot of redundancy in the parameters to this constructor: the {@code targetField} and {@code databaseFile} are
     * sufficient for the computation of the {@code geoLocationFieldTemplates}, which in turn are sufficient to compute the
     * {@code resolvedGeoLocationFields}. However, these values are all passed in to avoid recomputation when performing operations like
     * serialization/deserialization, renaming of the generated fields or replacing the child plan. When such are performed, newly generated
     * fields may be considered as different attributes, thus causing failures during local plan optimization.
     * @param source the source information
     * @param child the child logical plan
     * @param ipAddress the IP address expression
     * @param targetField the root of all generated fields
     * @param databaseFile the database file name to use for ip lookup
     * @param geoLocationFieldTemplates the geolocation field templates - name and data type
     * @param resolvedGeoLocationFields the resolved geolocation fields as attributes
     */
    public IpLookup(
        Source source,
        LogicalPlan child,
        Expression ipAddress,
        Attribute targetField,
        String databaseFile,
        Map<String, DataType> geoLocationFieldTemplates,
        List<Attribute> resolvedGeoLocationFields
    ) {
        super(source, child);
        this.ipAddress = ipAddress;
        this.targetField = targetField;
        this.databaseFile = databaseFile;
        this.geoLocationFieldTemplates = geoLocationFieldTemplates;
        this.resolvedGeoLocationFields = List.copyOf(resolvedGeoLocationFields);
    }

    /**
     * Deserializes an {@link IpLookup} instance from a {@link StreamInput}.
     *
     * @param in the input stream to read from
     * @throws IOException if an I/O error occurs
     */
    public IpLookup(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Attribute.class),
            in.readOptionalString(),
            in.readOrderedMap(StreamInput::readString, i -> i.readEnum(DataType.class)),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(ipAddress);
        out.writeNamedWriteable(targetField);
        out.writeOptionalString(databaseFile);
        out.writeMap(geoLocationFieldTemplates, StreamOutput::writeString, StreamOutput::writeEnum);
        out.writeNamedWriteableCollection(resolvedGeoLocationFields);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression ipAddress() {
        return ipAddress;
    }

    public Attribute targetField() {
        return targetField;
    }

    public String databaseFile() {
        return databaseFile;
    }

    public Map<String, DataType> geoLocationFieldTemplates() {
        return geoLocationFieldTemplates;
    }

    public List<Attribute> resolvedGeoLocationFields() {
        return resolvedGeoLocationFields;
    }

    @Override
    public List<Attribute> generatedAttributes() {
        return resolvedGeoLocationFields;
    }

    /**
     * By explicitly returning the references of the {@code ipAddress} expression, we implicitly exclude the generated fields from the
     * references that require resolution.
     * @return only the input expression references
     */
    @Override
    protected AttributeSet computeReferences() {
        return ipAddress.references();
    }

    @Override
    public IpLookup withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);

        List<Attribute> renamedFields = new ArrayList<>(newNames.size());
        for (int i = 0; i < newNames.size(); i++) {
            Attribute oldAttribute = resolvedGeoLocationFields.get(i);
            String newName = newNames.get(i);
            if (oldAttribute.name().equals(newName)) {
                renamedFields.add(oldAttribute);
            } else {
                renamedFields.add(oldAttribute.withName(newName).withId(new NameId()));
            }
        }

        return new IpLookup(source(), child(), ipAddress, targetField, databaseFile, geoLocationFieldTemplates, renamedFields);
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(generatedAttributes(), child().output());
    }

    @Override
    public IpLookup replaceChild(LogicalPlan newChild) {
        return new IpLookup(source(), newChild, ipAddress, targetField, databaseFile, geoLocationFieldTemplates, resolvedGeoLocationFields);
    }

    @Override
    public String telemetryLabel() {
        return "IP_LOOKUP";
    }

    @Override
    public boolean expressionsResolved() {
        return ipAddress.resolved();
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (ipAddress.resolved()) {
            DataType type = ipAddress.dataType();
            if (type != DataType.IP && DataType.isString(type) == false) {
                failures.add(fail(ipAddress, "Input for IP_LOOKUP must be of type [IP] or [String] but is [{}]", type.typeName()));
            }
        }
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(
            this,
            IpLookup::new,
            child(),
            ipAddress,
            targetField,
            databaseFile,
            geoLocationFieldTemplates,
            resolvedGeoLocationFields
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), ipAddress, targetField.name(), databaseFile, resolvedGeoLocationFields);
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
        IpLookup other = (IpLookup) obj;
        return Objects.equals(ipAddress, other.ipAddress)
            && Objects.equals(targetField.name(), other.targetField.name())
            && Objects.equals(databaseFile, other.databaseFile)
            && Objects.equals(resolvedGeoLocationFields, other.resolvedGeoLocationFields);
    }
}
