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
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * A {@code IpLookup} is a logical plan node that represents the ESQL
 * `IP_LOOKUP` command. It takes an IP address expression, looks up
 * geolocation information, and adds the results to a new target field.
 */
public class IpLookup extends UnaryPlan implements PostAnalysisVerificationAware, TelemetryAware, GeneratingPlan<IpLookup> {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "IpLookup", IpLookup::new);

    public static final String DATABASE_FILE_OPTION = "database_file";

    private final Expression ipAddress;
    private final Attribute targetField;
    private final String databaseFile;
    private final List<Attribute> resolvedGeoLocationFields;
    private final Map<String, DataType> geoLocationFieldTemplates;

    public IpLookup(Source source, LogicalPlan child, Expression ipAddress, Attribute targetField, String databaseFile) {
        super(source, child);
        this.ipAddress = ipAddress;
        this.targetField = targetField;
        this.databaseFile = databaseFile;
        // TODO - verify databaseFile exists and is accessible
        this.geoLocationFieldTemplates = getGeoLocationFieldTemplates(databaseFile);
        this.resolvedGeoLocationFields = geoLocationFieldTemplates.entrySet()
            .stream()
            .map(
                entry -> (Attribute) new ReferenceAttribute(
                    source,
                    null,
                    targetField.name() + "." + entry.getKey(),
                    entry.getValue(),
                    Nullability.TRUE,
                    null,
                    false
                )
            )
            .toList();
    }

    // Private constructor to be used by withGeneratedNames for rebuilding the node with new names and for deserialization.
    private IpLookup(
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

    // This is a stub that defines the schema contract for different database types.
    // Using LinkedHashMap to preserve order for consistent column output.
    private static Map<String, DataType> getGeoLocationFieldTemplates(String databaseName) {
        LinkedHashMap<String, DataType> fields = new LinkedHashMap<>();
        if ("GeoLite2-City.mmdb".equals(databaseName) || databaseName == null) {
            // Default contract for City databases
            fields.putLast("continent_name", DataType.KEYWORD);
            fields.putLast("country_iso_code", DataType.KEYWORD);
            fields.putLast("region_name", DataType.KEYWORD);
            fields.putLast("city_name", DataType.KEYWORD);
            fields.putLast("location", DataType.GEO_POINT);
        } else if ("GeoLite2-Country.mmdb".equals(databaseName)) {
            fields.putLast("continent_name", DataType.KEYWORD);
            fields.putLast("country_iso_code", DataType.KEYWORD);
        } else {
            // In a real implementation, more database types would be supported.
            throw new EsqlIllegalArgumentException("Unsupported GeoIP database file: [" + databaseName + "]");
        }
        return Collections.unmodifiableMap(fields);
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

    public List<Attribute> resolvedGeoLocationFields() {
        return resolvedGeoLocationFields;
    }

    public Map<String, DataType> geoLocationFieldTemplates() {
        return geoLocationFieldTemplates;
    }

    @Override
    public List<Attribute> generatedAttributes() {
        return resolvedGeoLocationFields;
    }

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

        // Reconstruct the node with the renamed fields using the private constructor
        return new IpLookup(source(), child(), ipAddress, targetField, databaseFile, geoLocationFieldTemplates, renamedFields);
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(generatedAttributes(), child().output());
    }

    @Override
    public IpLookup replaceChild(LogicalPlan newChild) {
        // When replacing a child, the derived fields should be re-calculated based on the same targetField and databaseFile
        // This constructor automatically recalculates resolvedGeoLocationFields.
        return new IpLookup(source(), newChild, ipAddress, targetField, databaseFile);
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
        return NodeInfo.create(this, IpLookup::new, child(), ipAddress, targetField, databaseFile);
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
        // Use targetField's name for equality to survive serialization.
        return Objects.equals(ipAddress, other.ipAddress)
            && Objects.equals(targetField.name(), other.targetField.name())
            && Objects.equals(databaseFile, other.databaseFile)
            && Objects.equals(resolvedGeoLocationFields, other.resolvedGeoLocationFields);
    }
}
